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

import static io.stargate.starter.Starter.STARTED_MESSAGE;
import static java.lang.management.ManagementFactory.getRuntimeMXBean;

import com.datastax.oss.driver.api.core.Version;
import io.stargate.it.storage.StargateParameters.Builder;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteResultHandler;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.LogOutputStream;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JUnit 5 extension for tests that need a Stargate OSGi container running in a separate JVM.
 *
 * <p>Note: this extension requires {@link ExternalStorage} to be activated as well. It is
 * recommended that test classes be annotated with {@link UseStargateContainer} to make sure both
 * extensions are activated in the right order.
 *
 * <p>Note: this extension does not support concurrent test execution.
 *
 * @see StargateSpec
 * @see StargateParameters
 */
public class StargateContainer extends ExternalResource<StargateSpec, StargateContainer.Container>
    implements ParameterResolver {
  private static final Logger LOG = LoggerFactory.getLogger(StargateContainer.class);

  private static final File LIB_DIR = initLibDir();
  private static final int PROCESS_WAIT_MINUTES =
      Integer.getInteger("stargate.test.process.wait.timeout.minutes", 10);

  // the first 10 addresses are reserved for storage nodes
  private static final AtomicInteger stargateAddressStart = new AtomicInteger(11);
  private static final AtomicInteger stargateInstanceSeq = new AtomicInteger();

  private static File initLibDir() {
    String dir = System.getProperty("stargate.libdir");
    if (dir == null) {
      throw new IllegalStateException("stargate.libdir system property is not set.");
    }

    return new File(dir);
  }

  public StargateContainer() {
    super(StargateSpec.class, "stargate-container", Namespace.GLOBAL);
  }

  private static StargateParameters parameters(StargateSpec spec, ExtensionContext context)
      throws Exception {
    Builder builder = StargateParameters.builder();

    String customizer = spec.parametersCustomizer().trim();
    if (!customizer.isEmpty()) {
      Object testInstance = context.getTestInstance().orElse(null);
      Class<?> testClass = context.getRequiredTestClass();
      Method method = testClass.getMethod(customizer, Builder.class);
      method.invoke(testInstance, builder);
    }

    return builder.build();
  }

  private Container container(ExtensionContext context) {
    return getResource(context)
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Stargate container has not been configured in " + context.getUniqueId()));
  }

  @Override
  protected boolean isShared(StargateSpec spec) {
    return spec.shared();
  }

  @Override
  protected Optional<Container> processResource(
      Container container, StargateSpec spec, ExtensionContext context) throws Exception {
    ClusterConnectionInfo backend =
        (ClusterConnectionInfo) context.getStore(Namespace.GLOBAL).get(ExternalStorage.STORE_KEY);
    Assertions.assertNotNull(
        backend, "Stargate backend is not available in " + context.getUniqueId());

    StargateParameters params = parameters(spec, context);

    if (container != null) {
      if (container.matches(backend, spec, params)) {
        LOG.info("Reusing matching Stargate container {} for {}", spec, context.getUniqueId());
        return Optional.empty();
      }

      LOG.info(
          "Closing old Stargate container due to spec mismatch within {}", context.getUniqueId());
      container.close();
    }

    LOG.info("Starting Stargate container with spec {} for {}", spec, context.getUniqueId());

    Container c = new Container(backend, spec, params);
    c.start();
    return Optional.of(c);
  }

  private boolean isConnectionInfo(ParameterContext parameterContext) {
    return parameterContext.getParameter().getType() == StargateConnectionInfo.class;
  }

  private boolean isEnvInfo(ParameterContext parameterContext) {
    return parameterContext.getParameter().getType() == StargateEnvironmentInfo.class;
  }

  @Override
  public boolean supportsParameter(ParameterContext pc, ExtensionContext ec)
      throws ParameterResolutionException {
    return isConnectionInfo(pc) || isEnvInfo(pc);
  }

  @Override
  public Object resolveParameter(ParameterContext pc, ExtensionContext ec)
      throws ParameterResolutionException {
    if (isEnvInfo(pc)) {
      return container(ec);
    } else if (isConnectionInfo(pc)) {
      return container(ec).nodes.get(0);
    }

    throw new IllegalStateException("Unknown parameter: " + pc);
  }

  private static boolean isDebug() {
    return getRuntimeMXBean().getInputArguments().toString().contains("-agentlib:jdwp");
  }

  protected static class Container extends ExternalResource.Holder
      implements StargateEnvironmentInfo, AutoCloseable {

    private final UUID id = UUID.randomUUID();
    private final ClusterConnectionInfo backend;
    private final StargateSpec spec;
    private final StargateParameters parameters;
    private final List<Node> nodes = new ArrayList<>();

    private Container(
        ClusterConnectionInfo backend, StargateSpec spec, StargateParameters parameters)
        throws Exception {
      this.backend = backend;
      this.spec = spec;
      this.parameters = parameters;

      int instanceNum = stargateInstanceSeq.getAndIncrement();

      Env env = new Env(spec.nodes());
      for (int i = 0; i < spec.nodes(); i++) {
        nodes.add(new Node(i, instanceNum, backend, env, parameters));
      }
    }

    private void start() {
      ShutdownHook.add(this);

      for (Node node : nodes) {
        node.start();
      }

      for (Node node : nodes) {
        node.awaitReady();
      }
    }

    private void stop() {
      ShutdownHook.remove(this);

      for (Node node : nodes) {
        node.stopNode();
      }

      for (Node node : nodes) {
        node.awaitExit();
      }
    }

    @Override
    public void close() {
      super.close();
      stop();
    }

    private boolean matches(
        ClusterConnectionInfo backend, StargateSpec spec, StargateParameters parameters) {
      return this.backend.id().equals(backend.id())
          && this.spec.equals(spec)
          && this.parameters.equals(parameters);
    }

    @Override
    public String id() {
      return id.toString();
    }

    @Override
    public List<? extends StargateConnectionInfo> nodes() {
      return nodes;
    }
  }

  private static class Node implements StargateConnectionInfo, ExecuteResultHandler {

    private final UUID id = UUID.randomUUID();
    private final int nodeIndex;
    private final int instanceNum;
    private final String listenAddress;
    private final String clusterName;
    private final CommandLine cmd;
    private final ExecuteWatchdog watchDog = new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT);
    private final CompletableFuture<Void> ready = new CompletableFuture<>();
    private final CountDownLatch exit = new CountDownLatch(1);
    private final int cqlPort;
    private final String datacenter;
    private final String rack;
    private final File cacheDir;

    private Node(
        int nodeIndex,
        int instanceNum,
        ClusterConnectionInfo backend,
        Env env,
        StargateParameters params)
        throws Exception {
      this.nodeIndex = nodeIndex;
      this.instanceNum = instanceNum;
      this.listenAddress = env.listenAddress(nodeIndex);
      this.cqlPort = env.cqlPort();
      this.clusterName = backend.clusterName();
      this.datacenter = backend.datacenter();
      this.rack = backend.rack();
      this.cacheDir = env.cacheDir(nodeIndex);

      cmd = new CommandLine("java");
      cmd.addArgument("-Dstargate.auth_api_enable_username_token=true");
      cmd.addArgument("-Dstargate.libdir=" + LIB_DIR.getAbsolutePath());
      cmd.addArgument("-Dstargate.bundle.cache.dir=" + cacheDir.getAbsolutePath());

      for (Entry<String, String> e : params.systemProperties().entrySet()) {
        cmd.addArgument("-D" + e.getKey() + "=" + e.getValue());
      }

      if (isDebug()) {
        int debuggerPort = 5100 + nodeIndex;
        cmd.addArgument(
            "-agentlib:jdwp=transport=dt_socket,server=n,suspend=y,"
                + "address=localhost:"
                + debuggerPort);
      }

      cmd.addArgument("-jar");
      cmd.addArgument(env.stargateJar().getAbsolutePath());
      cmd.addArgument("--cluster-seed");
      cmd.addArgument(backend.seedAddress());
      cmd.addArgument("--seed-port");
      cmd.addArgument(String.valueOf(backend.storagePort()));
      cmd.addArgument("--cluster-name");
      cmd.addArgument(clusterName);

      Version backendVersion = Version.parse(backend.clusterVersion());
      String version = String.format("%d.%d", backendVersion.getMajor(), backendVersion.getMinor());
      cmd.addArgument("--cluster-version");
      cmd.addArgument(version);

      cmd.addArgument("--dc");
      cmd.addArgument(datacenter);
      cmd.addArgument("--rack");
      cmd.addArgument(rack);

      if (backend.isDse()) {
        cmd.addArgument("--dse");
      }

      if (params.enableAuth()) {
        cmd.addArgument("--enable-auth");
      }

      cmd.addArgument("--listen");
      cmd.addArgument(listenAddress);
      cmd.addArgument("--bind-to-listen-address");
      cmd.addArgument("--cql-port");
      cmd.addArgument(String.valueOf(cqlPort));
      cmd.addArgument("--jmx-port");
      cmd.addArgument(String.valueOf(env.jmxPort(nodeIndex)));
    }

    private void start() {
      try {
        LogOutputStream out =
            new LogOutputStream() {
              @Override
              protected void processLine(String line, int logLevel) {
                if (line.contains(STARTED_MESSAGE)) {
                  ready.complete(null);
                }

                LOG.info("sg{}-{}> {}", instanceNum, nodeIndex, line);
              }
            };
        LogOutputStream err =
            new LogOutputStream() {
              @Override
              protected void processLine(String line, int logLevel) {
                LOG.error("sg{}-{}> {}", instanceNum, nodeIndex, line);
              }
            };

        Executor executor =
            new DefaultExecutor() {
              @Override
              protected Thread createThread(Runnable runnable, String name) {
                return super.createThread(
                    runnable, "stargate-runner-" + instanceNum + "-" + nodeIndex);
              }
            };

        executor.setExitValues(new int[] {0, 143}); // normal exit, normal termination by SIGTERM
        executor.setStreamHandler(new PumpStreamHandler(out, err));
        executor.setWatchdog(watchDog);

        try {
          LOG.info("Starting Stargate {}, node {}: {}", instanceNum, nodeIndex, cmd);

          executor.execute(cmd, this);

        } catch (IOException e) {
          LOG.info("Unable to run Stargate node {}: {}", nodeIndex, e.getMessage(), e);
        }
      } finally {
        exit.countDown();
      }
    }

    private void cleanup() {
      try {
        FileUtils.deleteDirectory(cacheDir);
      } catch (IOException e) {
        LOG.info("Unable to delete cache dir for Stargate node {}", nodeIndex, e);
      }
    }

    @Override
    public void onProcessComplete(int exitValue) {
      LOG.info(
          "Stargate {}, node {} existed with return code {}", instanceNum, nodeIndex, exitValue);
      cleanup();
      ready.complete(null); // just in case
      exit.countDown();
    }

    @Override
    public void onProcessFailed(ExecuteException e) {
      LOG.info(
          "Stargate {}, node {} failed with exception: {}",
          instanceNum,
          nodeIndex,
          e.getMessage(),
          e);
      cleanup();
      ready.completeExceptionally(e);
      exit.countDown();
    }

    private void stopNode() {
      LOG.info("Stopping Stargate {}, node {}", instanceNum, nodeIndex);
      watchDog.destroyProcess();
    }

    private void awaitReady() {
      try {
        ready.get(PROCESS_WAIT_MINUTES, TimeUnit.MINUTES);
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }

    private void awaitExit() {
      try {
        if (!exit.await(PROCESS_WAIT_MINUTES, TimeUnit.MINUTES)) {
          throw new IllegalStateException("Stargate node did not exit: " + nodeIndex);
        }
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public String id() {
      return id.toString();
    }

    @Override
    public String seedAddress() {
      return listenAddress;
    }

    @Override
    public int cqlPort() {
      return cqlPort;
    }

    @Override
    public String clusterName() {
      return clusterName;
    }

    @Override
    public String datacenter() {
      return datacenter;
    }

    @Override
    public String rack() {
      return rack;
    }
  }

  private static class Env {

    private final List<Integer> ports = new ArrayList<>();
    private final int addressStart;

    private Env(int nodeCount) throws IOException {
      // Note: do not reuse addresses
      addressStart = stargateAddressStart.getAndAdd(nodeCount);

      // Allocate `nodeCount` random ports
      List<ServerSocket> sockets = new ArrayList<>();
      for (int i = 0; i < nodeCount; i++) {
        ServerSocket socket = new ServerSocket(0);
        sockets.add(socket);
        ports.add(socket.getLocalPort());
      }

      for (ServerSocket socket : sockets) {
        socket.close();
      }
    }

    private String listenAddress(int index) {
      return "127.0.0." + (addressStart + index);
    }

    private int jmxPort(int index) {
      return ports.get(index);
    }

    private int cqlPort() {
      return 9043;
    }

    private File stargateJar() {
      File[] files = LIB_DIR.listFiles();
      Assertions.assertNotNull(files, "No files in " + LIB_DIR.getAbsolutePath());
      return Arrays.stream(files)
          .filter(f -> f.getName().startsWith("stargate-starter"))
          .filter(f -> f.getName().endsWith(".jar"))
          .findFirst()
          .orElseThrow(
              () ->
                  new IllegalStateException(
                      "Unable to find Stargate Starter jar in: " + LIB_DIR.getAbsolutePath()));
    }

    public File cacheDir(int nodeIndex) throws IOException {
      return Files.createTempDirectory("stargate-node-" + nodeIndex + "-felix-cache").toFile();
    }
  }
}
