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
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList.Builder;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteResultHandler;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.LogOutputStream;
import org.apache.commons.exec.PumpStreamHandler;
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
  private static final int PROCESS_WAIT_MINUTES =
      Integer.getInteger("stargate.test.process.wait.timeout.minutes", 10);
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
    LOG.warn("Error in {}", context.getUniqueId(), throwable);
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
    private final List<StorageNode> nodes;
    private final AtomicBoolean removed = new AtomicBoolean();

    public CcmCluster(ClusterSpec spec, ExtensionContext context) {
      super(spec);
      this.initSite = context.getUniqueId();
      int numNodes = spec.nodes();
      this.ccm =
          CcmBridge.builder()
              .withCassandraConfiguration("cluster_name", CLUSTER_NAME)
              .withNodes(numNodes)
              .build();

      Builder<StorageNode> nodes = ImmutableList.builder();
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
              node.stopNode();
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
          LOG.warn("Exception during CCM cluster shutdown: {}", e, e);
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

      Path configDir = configDirectory();

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

  private static class StorageNode implements ExecuteResultHandler {
    private final CcmCluster cluster;
    private final int nodeIndex;
    private final CommandLine cmd;
    private final ExecuteWatchdog watchDog = new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT);
    private final String readMessage;
    private final CompletableFuture<Void> ready = new CompletableFuture<>();
    private final CountDownLatch exit = new CountDownLatch(1);
    private final File cassandraConf;
    private final File nodeDir;
    private final File logDir;
    private final File binDir;

    private StorageNode(CcmCluster cluster, int nodeIndex) {
      this.cluster = cluster;
      this.nodeIndex = nodeIndex;

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
    }

    public void start() {
      try {
        LogOutputStream out =
            new LogOutputStream() {
              @Override
              protected void processLine(String line, int logLevel) {
                if (line.contains(readMessage)) {
                  ready.complete(null);
                }

                LOG.info("storage{}> {}", nodeIndex, line);
              }
            };
        LogOutputStream err =
            new LogOutputStream() {
              @Override
              protected void processLine(String line, int logLevel) {
                LOG.error("storage{}> {}", nodeIndex, line);
              }
            };

        Executor executor =
            new DefaultExecutor() {
              @Override
              protected Thread createThread(Runnable runnable, String name) {
                return super.createThread(runnable, "storage-runner-" + nodeIndex);
              }
            };

        executor.setExitValues(new int[] {0, 143}); // normal exit, normal termination by SIGTERM
        executor.setStreamHandler(new PumpStreamHandler(out, err));
        executor.setWatchdog(watchDog);

        try {
          LOG.info("Starting Storage node {}: {}", nodeIndex, cmd);

          ImmutableMap.Builder<String, String> env = ImmutableMap.builder();
          env.put("CASSANDRA_CONF", cassandraConf.getAbsolutePath());
          env.put("CASSANDRA_LOG_DIR", logDir.getAbsolutePath());

          if (cluster.isDse()) {
            env.put("DSE_HOME", cluster.installDir().getAbsolutePath());
            env.put(
                "CASSANDRA_HOME", cluster.installDir().getAbsolutePath() + "/resources/cassandra");
            env.put("TOMCAT_HOME", new File(nodeDir, "resources/tomcat").getAbsolutePath());
            env.put("DSE_LOG_ROOT", logDir.getAbsolutePath() + "/dse");
            env.put("DSE_CONF", new File(nodeDir, "resources/dse/conf").getAbsolutePath());
          } else {
            env.put("CASSANDRA_HOME", cluster.installDir().getAbsolutePath());
            env.put("CASSANDRA_INCLUDE", new File(binDir, "cassandra.in.sh").getAbsolutePath());
          }

          executor.execute(cmd, env.build(), this);

        } catch (IOException e) {
          LOG.info("Unable to run Stargate node {}: {}", nodeIndex, e.getMessage(), e);
        }
      } finally {
        exit.countDown();
      }
    }

    public void stopNode() {
      LOG.info("Stopping Storage node {}", nodeIndex);
      watchDog.destroyProcess();
    }

    @Override
    public void onProcessComplete(int exitValue) {
      LOG.info("Storage node {} existed with return code {}", nodeIndex, exitValue);
      ready.complete(null); // just in case
      exit.countDown();
    }

    @Override
    public void onProcessFailed(ExecuteException e) {
      LOG.info("Storage node {} failed with exception: {}", nodeIndex, e.getMessage(), e);
      ready.completeExceptionally(e);
      exit.countDown();
    }

    public void awaitReady() {
      try {
        ready.get(PROCESS_WAIT_MINUTES, TimeUnit.MINUTES);
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }

    public void awaitExit() {
      try {
        if (!exit.await(PROCESS_WAIT_MINUTES, TimeUnit.MINUTES)) {
          throw new IllegalStateException("Storage node did not exit: " + nodeIndex);
        }
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }
  }
}
