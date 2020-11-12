/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.it.storage;

import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.exec.LogOutputStream;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** JUnit 5 extension for tests that need a backend database cluster managed by {@code ccm}. */
public class ExternalStorage extends ExternalResource<ClusterSpec, ExternalStorage.Cluster>
    implements ParameterResolver, BeforeTestExecutionCallback, TestExecutionExceptionHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ExternalStorage.class);

  public static final String STORE_KEY = "stargate-storage";

  private static final String CCM_VERSION = "ccm.version";
  private static final boolean EXTERNAL_BACKEND =
      Boolean.getBoolean("stargate.test.backend.use.external");
  private static final String DATACENTER = System.getProperty("stargate.test.backend.dc", "dc1");
  private static final String CLUSTER_NAME =
      System.getProperty("stargate.test.backend.cluster_name", "Test_Cluster");
  private static final String CLUSTER_IMPL_CLASS_NAME =
      System.getProperty("stargate.test.backend.cluster.impl.class", CcmCluster.class.getName());

  static {
    String version = System.getProperty(CCM_VERSION, "3.11.8");
    System.setProperty(CCM_VERSION, version);
  }

  public ExternalStorage() {
    super(ClusterSpec.class, STORE_KEY, Namespace.GLOBAL);
  }

  private Cluster createCluster(ClusterSpec spec, ExtensionContext context) {
    try {
      Class<?> cl = Class.forName(CLUSTER_IMPL_CLASS_NAME);
      Constructor<?> constructor = cl.getConstructor(ClusterSpec.class, ExtensionContext.class);
      return (Cluster) constructor.newInstance(spec, context);
    } catch (Throwable e) {
      LOG.error("Unable to create cluster object of type {}", CLUSTER_IMPL_CLASS_NAME, e);
      throw new IllegalStateException(e);
    }
  }

  @Override
  protected boolean isShared(ClusterSpec spec) {
    return spec.shared();
  }

  @Override
  protected Optional<Cluster> processResource(
      Cluster current, ClusterSpec spec, ExtensionContext context) {
    if (current != null) {
      if (current.spec.equals(spec)) {
        LOG.info("Reusing matching storage cluster {} for {}", spec, context.getUniqueId());
        return Optional.empty();
      }

      LOG.info("Closing old cluster due to spec mismatch within {}", context.getUniqueId());
      current.close();
    }

    LOG.info("Creating storage cluster {} for {}", spec, context.getUniqueId());

    Cluster c = createCluster(spec, context);
    c.start();
    return Optional.of(c);
  }

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return parameterContext.getParameter().getType() == ClusterConnectionInfo.class;
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return getResource(extensionContext)
        .orElseThrow(() -> new IllegalStateException("Cluster not available"));
  }

  @Override
  public void beforeTestExecution(ExtensionContext context) {
    LOG.info(
        "About to run {} with storage cluster {}",
        context.getUniqueId(),
        getResource(context).map(Cluster::infoForTestLog).orElse("[missing]"));
  }

  @Override
  public void handleTestExecutionException(ExtensionContext context, Throwable throwable)
      throws Throwable {
    getResource(context).ifPresent(Cluster::markError);
    throw throwable;
  }

  public abstract static class Cluster extends ExternalResource.Holder
      implements ClusterConnectionInfo, AutoCloseable {

    private final UUID id = UUID.randomUUID();
    private final ClusterSpec spec;
    private final AtomicBoolean errorInTest = new AtomicBoolean();

    protected Cluster(ClusterSpec spec) {
      this.spec = spec;
    }

    @Override
    public String id() {
      return id.toString();
    }

    public abstract void start();

    public abstract String infoForTestLog();

    private void markError() {
      errorInTest.set(true);
    }

    public boolean errorsDetected() {
      return errorInTest.get();
    }
  }

  public static class CcmCluster extends Cluster {

    private final String initSite;
    private final CcmBridge ccm;
    private final AtomicBoolean removed = new AtomicBoolean();

    public CcmCluster(ClusterSpec spec, ExtensionContext context) {
      super(spec);
      this.initSite = context.getUniqueId();
      this.ccm =
          CcmBridge.builder()
              .withCassandraConfiguration("cluster_name", CLUSTER_NAME)
              .withNodes(spec.nodes())
              .build();
    }

    @Override
    public void start() {
      if (!EXTERNAL_BACKEND) {
        ccm.create();
        ccm.start();

        ShutdownHook.add(this);

        LOG.info(
            "Storage cluster requested by {} has been started with version {}",
            initSite,
            clusterVersion());
      }
    }

    @Override
    public void close() {
      super.close();
      stop();
    }

    public void stop() {
      if (!EXTERNAL_BACKEND) {
        ShutdownHook.remove(this);

        try {
          if (removed.compareAndSet(false, true)) {
            dumpLogs();
            ccm.remove();
            LOG.info(
                "Storage cluster (version {}) that was requested by {} has been removed.",
                clusterVersion(),
                initSite);
          }
        } catch (Exception e) {
          // This should not affect test result validity, hence logging as WARN
          LOG.warn("Exception during CCM cluster shutdown: {}", e.toString(), e);
        }
      }
    }

    @Override
    public String infoForTestLog() {
      return "" + (isDse() ? "DSE " : "Cassandra ") + clusterVersion();
    }

    private void dumpLogs() {
      if (!errorsDetected()) {
        return;
      }

      LOG.warn("Dumping storage logs due to previous test failures.");

      Path configDir;
      try {
        Field f = ccm.getClass().getDeclaredField("configDirectory");
        f.setAccessible(true);
        configDir = (Path) f.get(ccm);
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }

      Collection<File> files = FileUtils.listFiles(configDir.toFile(), new String[] {"log"}, true);
      for (File file : files) {
        String relPath = configDir.relativize(file.toPath()).toString();
        if (!relPath.contains(File.separator + "logs" + File.separator)) {
          continue;
        }

        try (LogOutputStream dumper =
            new LogOutputStream() {
              @Override
              protected void processLine(String line, int logLevel) {
                LOG.info("storage log: {}>> {}", relPath, line);
              }
            }) {
          FileUtils.copyFile(file, dumper);
        } catch (IOException e) {
          throw new IllegalStateException(e);
        }
      }
    }

    @Override
    public String seedAddress() {
      return "127.0.0.1";
    }

    @Override
    public int storagePort() {
      return 7000;
    }

    @Override
    public int cqlPort() {
      return 9042;
    }

    @Override
    public String clusterName() {
      return CLUSTER_NAME;
    }

    @Override
    public String clusterVersion() {
      Version version = ccm.getDseVersion().orElse(ccm.getCassandraVersion());
      return String.format("%d.%d", version.getMajor(), version.getMinor());
    }

    @Override
    public boolean isDse() {
      return ccm.getDseVersion().isPresent();
    }

    @Override
    public String datacenter() {
      return DATACENTER;
    }

    @Override
    public String rack() {
      return "rack1";
    }
  }
}
