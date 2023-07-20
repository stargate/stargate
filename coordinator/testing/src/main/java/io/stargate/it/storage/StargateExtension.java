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

import com.datastax.oss.driver.shaded.guava.common.io.Resources;
import io.stargate.it.exec.OutputListener;
import io.stargate.it.exec.ProcessRunner;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.cassandra.stargate.config.Config;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.io.FileUtils;
import org.assertj.core.util.Strings;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

/**
 * JUnit 5 extension for tests that need a Stargate coordinator node running in a separate JVM.
 *
 * <p>Note: this extension requires {@link ExternalStorage} to be activated as well. It is
 * recommended that test classes be annotated with {@link UseStargateCoordinator} to make sure both
 * extensions are activated in the right order.
 *
 * <p>Note: this extension does not support concurrent test execution.
 *
 * @see StargateSpec
 * @see StargateParameters
 */
public class StargateExtension extends ExternalResource<StargateSpec, StargateExtension.Coordinator>
    implements ParameterResolver {
  private static final Logger LOG = LoggerFactory.getLogger(StargateExtension.class);

  private static final String ARGS_PROVIDER_CLASS_NAME =
      System.getProperty("stargate.test.args.provider.class", ArgumentProviderImpl.class.getName());

  private static final String PERSISTENCE_MODULE =
      System.getProperty("stargate.test.persistence.module");
  public static final File LIB_DIR = initLibDir();
  private static final int MAX_NODES = 20;

  /**
   * Preallocate JMX ports to guarantee Stargate nodes receive a unique port. This includes cases
   * where multiple concurrent Stargate clusters are created during parallel execution of tests.
   */
  private static final Queue<Integer> jmxPorts = initJmxPorts(MAX_NODES);

  private static final AtomicInteger stargateAddressStart = new AtomicInteger(1);
  private static final AtomicInteger stargateInstanceSeq = new AtomicInteger();

  public static final String STORE_KEY = "stargate-container";

  private static File initLibDir() {
    String dir = System.getProperty("stargate.libdir");
    if (dir == null) {
      throw new IllegalStateException("stargate.libdir system property is not set.");
    }

    return new File(dir);
  }

  private static File starterJar() {
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

  private static Queue<Integer> initJmxPorts(int maxNodeCount) {
    Queue<Integer> ports = new ConcurrentLinkedQueue<>();

    try {
      List<ServerSocket> sockets = new ArrayList<>();
      for (int i = 0; i < maxNodeCount; i++) {
        ServerSocket socket = new ServerSocket(0);
        sockets.add(socket);
        ports.add(socket.getLocalPort());
      }
      for (ServerSocket socket : sockets) {
        socket.close();
      }
    } catch (IOException e) {
      LOG.error("Unable to preallocate JMX ports", e);
      throw new UncheckedIOException(e);
    }

    return ports;
  }

  public StargateExtension() {
    super(StargateSpec.class, STORE_KEY, Namespace.GLOBAL);
  }

  private static StargateParameters parameters(StargateSpec spec, ExtensionContext context)
      throws Exception {
    StargateParameters.Builder builder = StargateParameters.builder();

    String customizer = spec.parametersCustomizer().trim();
    if (!customizer.isEmpty()) {
      Object testInstance = context.getTestInstance().orElse(null);
      Class<?> testClass = context.getRequiredTestClass();
      Method method = testClass.getMethod(customizer, StargateParameters.Builder.class);
      method.invoke(testInstance, builder);
    }

    return builder.build();
  }

  private Coordinator container(ExtensionContext context) {
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
  protected Optional<Coordinator> processResource(
      Coordinator container, StargateSpec spec, ExtensionContext context) throws Exception {
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

    Coordinator c = new Coordinator(backend, spec, params);
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
    String args = getRuntimeMXBean().getInputArguments().toString();
    return args.contains("-agentlib:jdwp") || args.contains("-Xrunjdwp");
  }

  protected static class Coordinator extends ExternalResource.Holder
      implements StargateEnvironmentInfo, AutoCloseable {

    private final UUID id = UUID.randomUUID();
    private final ClusterConnectionInfo backend;
    private final StargateSpec spec;
    private final StargateParameters parameters;
    private final List<Node> nodes = new ArrayList<>();
    private final int instanceNum;
    private final Env env;

    private Coordinator(
        ClusterConnectionInfo backend, StargateSpec spec, StargateParameters parameters)
        throws Exception {
      this.backend = backend;
      this.spec = spec;
      this.parameters = parameters;

      instanceNum = stargateInstanceSeq.getAndIncrement();

      env = new Env();
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
        node.stop();
      }

      for (Node node : nodes) {
        node.awaitExit();
      }
    }

    @Override
    public void close() {
      super.close();
      stop();
      env.close();
    }

    private boolean matches(
        ClusterConnectionInfo backend, StargateSpec spec, StargateParameters parameters) {
      return this.backend.id().equals(backend.id())
          && this.spec.nodes() == spec.nodes()
          && this.spec.shared() == spec.shared()
          && this.parameters.equals(parameters);
    }

    @Override
    public String id() {
      return id.toString();
    }

    @Override
    public File starterJarFile() {
      return starterJar();
    }

    @Override
    public List<? extends StargateConnectionInfo> nodes() {
      return nodes;
    }

    @Override
    public StargateConnectionInfo addNode() throws Exception {
      if (spec.shared()) {
        throw new UnsupportedOperationException(
            "Adding a node to a shared cluster is not supported");
      }
      Node node = new Node(nodes.size(), instanceNum, backend, env, parameters);
      nodes.add(node);
      node.start();
      node.awaitReady();
      return node;
    }

    @Override
    public void removeNode(StargateConnectionInfo node) {
      if (spec.shared()) {
        throw new UnsupportedOperationException(
            "Removing a node from a shared cluster is not supported");
      }
      Node internalNode = (Node) node;
      internalNode.stop();
      internalNode.awaitExit();
      nodes.remove(node);
    }

    @Override
    public void addStdOutListener(OutputListener listener) {
      for (Node node : nodes) {
        node.addStdOutListener(listener);
      }
    }

    @Override
    public void removeStdOutListener(OutputListener listener) {
      for (Node node : nodes) {
        node.removeStdOutListener(listener);
      }
    }
  }

  private static class Node extends ProcessRunner implements StargateConnectionInfo {

    private static final String SERVER_KEYSTORE_PATH = "/server.keystore";
    private static final String SERVER_KEYSTORE_PASSWORD = "fakePasswordForTests";

    private static final String SERVER_TRUSTSTORE_PATH = "/server.truststore";

    private static final String SERVER_TRUSTSTORE_PASSWORD = "fakePasswordForTests";

    private final UUID id = UUID.randomUUID();
    private final int nodeIndex;
    private final String listenAddress;
    private final String clusterName;
    private final CommandLine cmd;
    private final int cqlPort;
    private final int bridgePort;
    private final int jmxPort;
    private final String datacenter;
    private final String rack;
    private final File cacheDir;

    private final File cqlConfigFile;

    private Node(
        int nodeIndex,
        int instanceNum,
        ClusterConnectionInfo backend,
        Env env,
        StargateParameters params)
        throws Exception {
      super("Stargate", instanceNum, nodeIndex);
      this.nodeIndex = nodeIndex;
      this.listenAddress = env.listenAddress(nodeIndex);
      this.cqlPort = env.cqlPort();
      this.bridgePort = env.bridgePort();
      this.jmxPort = env.jmxPort(nodeIndex);
      this.clusterName = backend.clusterName();
      this.datacenter = backend.datacenter();
      this.rack = backend.rack();
      this.cacheDir = env.cacheDir(nodeIndex);
      this.cqlConfigFile = env.cqlConfigFile(nodeIndex);

      cmd = new CommandLine("java");
      cmd.addArgument("-Dstargate.auth_api_enable_username_token=true");
      cmd.addArgument("-Dstargate.libdir=" + LIB_DIR.getAbsolutePath());
      cmd.addArgument("-Dstargate.bundle.cache.dir=" + cacheDir.getAbsolutePath());

      // Java 11+ requires these flags to allow reflection to work
      cmd.addArgument("--add-exports");
      cmd.addArgument("java.base/jdk.internal.ref=ALL-UNNAMED");
      cmd.addArgument("--add-exports");
      cmd.addArgument("java.base/jdk.internal.misc=ALL-UNNAMED");

      if (backend.isDse()) {
        cmd.addArgument("-Dstargate.request_timeout_in_ms=60000");
        cmd.addArgument("-Dstargate.write_request_timeout_in_ms=60000");
        cmd.addArgument("-Dstargate.read_request_timeout_in_ms=60000");
      }

      cmd.addArgument("-Dstargate.enable_user_defined_functions=true");

      for (Entry<String, String> e : params.systemProperties().entrySet()) {
        cmd.addArgument("-D" + e.getKey() + "=" + e.getValue());
      }

      if (params.sslForCqlParameters().enabled()) {
        Yaml yaml = new Yaml();
        Config config = new Config();
        config.client_encryption_options =
            config
                .client_encryption_options
                .withEnabled(true)
                .withOptional(params.sslForCqlParameters().optional())
                .withKeyStore(createTempStore(SERVER_KEYSTORE_PATH).getAbsolutePath())
                .withKeyStorePassword(SERVER_KEYSTORE_PASSWORD);

        if (params.sslForCqlParameters().requireClientCertificates()) {
          config.client_encryption_options =
              config
                  .client_encryption_options
                  .withRequireClientAuth(true)
                  .withTrustStore(createTempStore(SERVER_TRUSTSTORE_PATH).getAbsolutePath())
                  .withTrustStorePassword(SERVER_TRUSTSTORE_PASSWORD);
        }

        yaml.dump(config, new PrintWriter(cqlConfigFile));
        cmd.addArgument("-Dstargate.cql.config_path=" + cqlConfigFile.getAbsolutePath());
      }

      if (isDebug()) {
        int debuggerPort = 5100 + nodeIndex;
        cmd.addArgument(
            "-agentlib:jdwp=transport=dt_socket,server=n,suspend=y,"
                + "address=localhost:"
                + debuggerPort);
      }

      for (String arg : args(backend)) {
        cmd.addArgument(arg);
      }

      if (params.enableAuth()) {
        cmd.addArgument("--enable-auth");
      }

      if (params.useProxyProtocol()) {
        cmd.addArgument("--use-proxy-protocol");
        if (!Strings.isNullOrEmpty(params.proxyDnsName())) {
          cmd.addArgument("--proxy-dns-name");
          cmd.addArgument(params.proxyDnsName());
        }
        cmd.addArgument("--proxy-port");
        cmd.addArgument(String.valueOf(params.proxyPort()));
      }

      cmd.addArgument("--listen");
      cmd.addArgument(listenAddress);
      cmd.addArgument("--bind-to-listen-address");
      cmd.addArgument("--cql-port");
      cmd.addArgument(String.valueOf(cqlPort));
      cmd.addArgument("--jmx-port");
      cmd.addArgument(String.valueOf(jmxPort));

      addStdOutListener(
          (node, line) -> {
            if (line.contains(STARTED_MESSAGE)) {
              ready();
            }
          });
    }

    private Collection<String> args(ClusterConnectionInfo backend) {
      try {
        Class<?> argsProviderClass = Class.forName(ARGS_PROVIDER_CLASS_NAME);
        ArgumentProvider provider =
            (ArgumentProvider) argsProviderClass.getDeclaredConstructor().newInstance();
        return provider.commandArguments(backend);
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }

    private void start() {
      start(cmd, Collections.emptyMap());
    }

    @Override
    protected void cleanup() {
      try {
        FileUtils.deleteDirectory(cacheDir);
        FileUtils.deleteDirectory(cqlConfigFile.getParentFile());
      } catch (IOException e) {
        LOG.info("Unable to delete cache dir for Stargate node {}", nodeIndex, e);
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
    public int bridgePort() {
      return bridgePort;
    }

    @Override
    public int jmxPort() {
      return jmxPort;
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

    private File createTempStore(String path) {
      File f = null;
      try (OutputStream os =
          Files.newOutputStream(
              (f = File.createTempFile("cql", ".store", this.cqlConfigFile.getParentFile()))
                  .toPath())) {
        Resources.copy(Objects.requireNonNull(StargateExtension.class.getResource(path)), os);
      } catch (IOException e) {
        LOG.warn("Failure to write keystore, SSL-enabled servers may fail to start.", e);
      }
      return f;
    }
  }

  private static class Env implements Closeable {
    private final Map<Integer, Integer> ports = new HashMap<>();
    private final Map<Integer, String> listenAddresses = new HashMap<>();

    private synchronized String listenAddress(int index) {
      // Note: 127.0.1.N addresses are used by proxy protocol testing,
      // so we allocate from the 127.0.2.X range here, to avoid conflicts with other
      // services that may be listening on the common range of 127.0.0.Y addresses.
      return listenAddresses.computeIfAbsent(
          index, i -> "127.0.2." + stargateAddressStart.getAndIncrement());
    }

    private synchronized int jmxPort(int index) {
      return ports.computeIfAbsent(
          index,
          i -> {
            Integer port = jmxPorts.poll();
            if (port == null) {
              throw new AssertionError(
                  String.format("Tests using too many Stargate nodes (%d maximum)", MAX_NODES));
            }
            return port;
          });
    }

    private int cqlPort() {
      return 9043;
    }

    private int bridgePort() {
      return 8091;
    }

    public File cacheDir(int nodeIndex) throws IOException {
      return Files.createTempDirectory("stargate-node-" + nodeIndex + "-felix-cache").toFile();
    }

    public File cqlConfigFile(int nodeIndex) throws IOException {
      File dir = Files.createTempDirectory("stargate-node-" + nodeIndex + "-cql-config").toFile();
      return new File(dir, "cql.yaml");
    }

    @Override
    public synchronized void close() {
      jmxPorts.addAll(ports.values());
    }
  }

  public interface ArgumentProvider {
    Collection<String> commandArguments(ClusterConnectionInfo backend);
  }

  public static class ArgumentProviderImpl implements ArgumentProvider {

    @Override
    public Collection<String> commandArguments(ClusterConnectionInfo backend) {
      Collection<String> args = new ArrayList<>();
      args.add("-ea");
      args.add("-jar");
      args.add(starterJar().getAbsolutePath());
      args.add("--cluster-seed");
      args.add(backend.seedAddress());
      args.add("--seed-port");
      args.add(String.valueOf(backend.storagePort()));
      args.add("--cluster-name");
      args.add(backend.clusterName());

      if (PERSISTENCE_MODULE != null) {
        args.add("--persistence-module");
        args.add(PERSISTENCE_MODULE);
      }
      ;

      args.add("--dc");
      args.add(backend.datacenter());
      args.add("--rack");
      args.add(backend.rack());

      if (backend.isDse()) {
        args.add("--dse");
      }

      return args;
    }
  }
}
