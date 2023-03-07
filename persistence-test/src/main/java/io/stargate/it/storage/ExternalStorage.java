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
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.stargate.it.exec.ProcessRunner;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.LogOutputStream;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;
import org.opentest4j.TestAbortedException;
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
    String version = System.getProperty(CCM_VERSION, "3.11.14");
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
    // 24-Sep-2021, tatu: When using "assume()" methods for dynamic disabling/skipping
    //    of tests, we still get an exception -- but those are not fails, need to filter out
    //    so that we do not unnecessarily dump output logs
    if ((throwable instanceof TestAbortedException) // opentest4j
        || "AssumptionViolatedException".equals(throwable.getClass().getSimpleName()) // junit
    ) {
      LOG.warn(
          "Test skipped due to failed Assume in {}: {}",
          context.getUniqueId(),
          throwable.getMessage());
      getResource(context).ifPresent(Cluster::markSkippedTest);
    } else {
      LOG.warn("Test error in {}", context.getUniqueId(), throwable);
      getResource(context).ifPresent(Cluster::markErroredTest);
    }
    throw throwable;
  }

  public abstract static class Cluster extends ExternalResource.Holder
      implements ClusterConnectionInfo, AutoCloseable {

    private final UUID id = UUID.randomUUID();
    private final ClusterSpec spec;
    private final AtomicInteger errorsInTest = new AtomicInteger(0);
    private final AtomicInteger skippedTests = new AtomicInteger(0);

    protected Cluster(ClusterSpec spec) {
      this.spec = spec;
    }

    @Override
    public String id() {
      return id.toString();
    }

    public abstract void start();

    public abstract String infoForTestLog();

    private void markSkippedTest() {
      skippedTests.incrementAndGet();
    }

    private void markErroredTest() {
      errorsInTest.incrementAndGet();
    }

    public int testsSkipped() {
      return skippedTests.get();
    }

    public int errorsDetected() {
      return errorsInTest.get();
    }
  }

  public static class CcmCluster extends Cluster {

    private final String initSite;
    private final CcmBridge ccm;
    private final List<StorageNode> nodes;
    private final AtomicBoolean removed = new AtomicBoolean();

    public CcmCluster(ClusterSpec spec, ExtensionContext context) {
      this(spec, Collections.emptyMap(), Collections.emptyMap(), context);
    }

    public CcmCluster(
        ClusterSpec spec,
        Map<String, Object> cassandraConfig,
        Map<String, Object> dseConfig,
        ExtensionContext context) {
      super(spec);
      this.initSite = context.getUniqueId();
      int numNodes = spec.nodes();

      CcmBridge.Builder builder =
          CcmBridge.builder()
              .withCassandraConfiguration("cluster_name", CLUSTER_NAME)
              .withCassandraConfiguration("metadata_directory", "$HOME/.stargate-test/cassandra/metadata")
              .withNodes(numNodes);

      for (Entry<String, Object> e : cassandraConfig.entrySet()) {
        builder = builder.withCassandraConfiguration(e.getKey(), e.getValue());
      }

      for (Entry<String, Object> e : dseConfig.entrySet()) {
        builder = builder.withDseConfiguration(e.getKey(), e.getValue());
      }

      this.ccm = builder.build();

      ImmutableList.Builder<StorageNode> nodes = ImmutableList.builder();
      for (int i = 0; i < numNodes; i++) {
        nodes.add(new StorageNode(this, i));
      }
      this.nodes = nodes.build();
    }

    private Path configDirectory() {
      try {
        Field f = ccm.getClass().getDeclaredField("configDirectory");
        f.setAccessible(true);
        return (Path) f.get(ccm);
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }

    private File clusterDir() {
      return new File(configDirectory().toFile(), "ccm_1");
    }

    private File installDir() {
      File ccmConfig = new File(clusterDir(), "cluster.conf");
      ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
      try {
        Map<?, ?> config = mapper.readValue(ccmConfig, Map.class);
        String dir = (String) config.get("install_dir");
        return new File(dir);
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public void start() {
      if (!EXTERNAL_BACKEND) {
        ccm.create();

        ShutdownHook.add(this);

        for (StorageNode node : nodes) {
          node.start();
        }

        for (StorageNode node : nodes) {
          node.awaitReady();
        }

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

            for (StorageNode node : nodes) {
              node.stop();
            }

            for (StorageNode node : nodes) {
              node.awaitExit();
            }

            ccm.remove();

            LOG.info(
                "Storage cluster (version {}) that was requested by {} has been removed.",
                clusterVersion(),
                initSite);
          }
        } catch (Exception e) {
          // This should not affect test result validity, hence logging as WARN
          LOG.warn("Exception during CCM cluster shutdown: " + e.getMessage(), e);
        }
      }
    }

    @Override
    public String infoForTestLog() {
      return "" + (isDse() ? "DSE " : "Cassandra ") + clusterVersion();
    }

    private void dumpLogs() {
      final int errors = errorsDetected();
      if (errors == 0) {
        LOG.info(
            "Skipping dumping storage cluster ({}) logs: no test failures ({} tests skipped).",
            infoForTestLog(),
            testsSkipped());
        return;
      }

      LOG.warn(
          "Dumping storage cluster ({}) logs due to {} test failures ({} tests skipped).",
          infoForTestLog(),
          errors,
          testsSkipped());
      Path configDir = configDirectory();

      final Collection<File> files =
          FileUtils.listFiles(configDir.toFile(), new String[] {"log"}, true);
      int filesProcessed = 0;
      final AtomicInteger printedLines = new AtomicInteger(0);
      final AtomicInteger skippedLines = new AtomicInteger(0);
      for (File file : files) {
        String relPath = configDir.relativize(file.toPath()).toString();
        if (!relPath.contains(File.separator + "logs" + File.separator)) {
          continue;
        }

        ++filesProcessed;
        try (LogOutputStream dumper =
            new LogOutputStream() {
              @Override
              protected void processLine(String line, int logLevel) {
                // Let's filter out DEBUG entries: unfortunately "logLevel" is always default 999
                // so need to just check prefix of the line itself
                if (line.startsWith("DEBUG ")) {
                  skippedLines.incrementAndGet();
                } else {
                  printedLines.incrementAndGet();
                  LOG.info("storage log: {}>> {}", relPath, line);
                }
              }
            }) {
          FileUtils.copyFile(file, dumper);
        } catch (IOException e) {
          throw new IllegalStateException(e);
        }
      }
      LOG.warn(
          "Finished dumping storage logs ({} files): printed {} lines, skipped {} DEBUG lines",
          filesProcessed,
          printedLines.get(),
          skippedLines.get());
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

  private static class StorageNode extends ProcessRunner {
    private final CcmCluster cluster;
    private final CommandLine cmd;
    private final String readMessage;
    private final File cassandraConf;
    private final File nodeDir;
    private final File logDir;
    private final File binDir;

    private StorageNode(CcmCluster cluster, int nodeIndex) {
      super("Storage", 0, nodeIndex);
      this.cluster = cluster;

      Path configDir = cluster.configDirectory();
      File clusterDir = new File(configDir.toFile(), "ccm_1");
      nodeDir = new File(clusterDir, "node" + (1 + nodeIndex));

      if (cluster.isDse()) {
        cassandraConf = new File(nodeDir, "resources/cassandra/conf");
      } else {
        cassandraConf = new File(nodeDir, "conf");
      }

      logDir = new File(nodeDir, "logs");
      binDir = new File(nodeDir, "bin");
      File startScript;
      if (cluster.isDse()) {
        readMessage = "DSE startup complete";
        startScript = new File(binDir, "dse");
      } else {
        readMessage = "Starting listening for CQL clients";
        startScript = new File(binDir, "cassandra");
      }

      cmd = new CommandLine(startScript.getAbsolutePath());

      if (cluster.isDse()) {
        cmd.addArgument("cassandra");
      }

      cmd.addArgument("-f"); // run in the foreground
      cmd.addArgument("-Dcassandra.logdir=" + logDir.getAbsolutePath());
      cmd.addArgument("-Dcassandra.boot_without_jna=true");

      if (cluster.isDse()) {
        cmd.addArgument("-Dcassandra.migration_task_wait_in_seconds=4");
      }

      addStdOutListener(
          (node, line) -> {
            if (line.contains(readMessage)) {
              ready();
            }
          });
    }

    public void start() {
      ImmutableMap.Builder<String, String> env = ImmutableMap.builder();
      env.put("CASSANDRA_CONF", cassandraConf.getAbsolutePath());
      env.put("CASSANDRA_LOG_DIR", logDir.getAbsolutePath());

      if (cluster.isDse()) {
        env.put("DSE_HOME", cluster.installDir().getAbsolutePath());
        env.put("CASSANDRA_HOME", cluster.installDir().getAbsolutePath() + "/resources/cassandra");
        env.put("TOMCAT_HOME", new File(nodeDir, "resources/tomcat").getAbsolutePath());
        env.put("DSE_LOG_ROOT", logDir.getAbsolutePath() + "/dse");
        env.put("DSE_CONF", new File(nodeDir, "resources/dse/conf").getAbsolutePath());
      } else {
        env.put("CASSANDRA_HOME", cluster.installDir().getAbsolutePath());
        env.put("CASSANDRA_INCLUDE", new File(binDir, "cassandra.in.sh").getAbsolutePath());
      }

      start(cmd, env.build());
    }
  }
}
