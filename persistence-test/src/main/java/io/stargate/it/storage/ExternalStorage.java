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
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** JUnit 5 extension for tests that need a backend database cluster managed by {@code ccm}. */
public class ExternalStorage extends ExternalResource<ClusterSpec, ExternalStorage.Cluster>
    implements ParameterResolver, BeforeTestExecutionCallback {

  private static final Logger LOG = LoggerFactory.getLogger(ExternalStorage.class);

  public static final String STORE_KEY = "stargate-storage";

  private static final String CCM_VERSION = "ccm.version";
  private static final boolean EXTERNAL_BACKEND =
      Boolean.getBoolean("stargate.test.backend.use.external");
  private static final String DATACENTER = System.getProperty("stargate.test.backend.dc", "dc1");
  private static final String CLUSTER_NAME =
      System.getProperty("stargate.test.backend.cluster_name", "Test_Cluster");

  static {
    String version = System.getProperty(CCM_VERSION, "3.11.8");
    System.setProperty(CCM_VERSION, version);
  }

  public ExternalStorage() {
    super(ClusterSpec.class, STORE_KEY, Namespace.GLOBAL);
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

    Cluster c = new Cluster(spec, context.getUniqueId());
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
        "About to run {} with storage cluster version {}",
        context.getUniqueId(),
        getResource(context).map(Cluster::clusterVersion).orElse("[missing]"));
  }

  protected static class Cluster extends ExternalResource.Holder
      implements ClusterConnectionInfo, AutoCloseable {

    private final UUID id = UUID.randomUUID();
    private final ClusterSpec spec;
    private final String initSite;
    private final CcmBridge ccm;
    private final AtomicBoolean removed = new AtomicBoolean();

    private Cluster(ClusterSpec spec, String displayName) {
      this.spec = spec;
      this.initSite = displayName;
      this.ccm =
          CcmBridge.builder()
              .withCassandraConfiguration("cluster_name", CLUSTER_NAME)
              .withNodes(spec.nodes())
              .build();
    }

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
    public String id() {
      return id.toString();
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
