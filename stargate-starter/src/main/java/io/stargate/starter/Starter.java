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
package io.stargate.starter;

import com.github.rvesse.airline.HelpOption;
import com.github.rvesse.airline.SingleCommand;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.Parser;
import com.github.rvesse.airline.annotations.help.License;
import com.github.rvesse.airline.annotations.restrictions.Port;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.github.rvesse.airline.help.cli.CliCommandUsageGenerator;
import com.github.rvesse.airline.model.OptionMetadata;
import com.github.rvesse.airline.parser.ParseResult;
import com.github.rvesse.airline.parser.ParseState;
import com.github.rvesse.airline.parser.errors.ParseException;
import com.github.rvesse.airline.parser.options.AbstractOptionParser;
import io.stargate.starter.Starter.NodeToolOptionParser;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.inject.Inject;
import org.apache.commons.collections4.iterators.PeekingIterator;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.felix.framework.Felix;
import org.apache.felix.framework.util.FelixConstants;
import org.apache.felix.resolver.Logger;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleException;
import org.osgi.framework.Constants;
import org.osgi.framework.FrameworkEvent;
import org.osgi.framework.FrameworkListener;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceReference;

/** Starts a OSGi container and installs and starts all needed bundles */
@License(url = "https://www.apache.org/licenses/LICENSE-2.0")
@Command(name = "Stargate")
@Parser(defaultParsersFirst = false, optionParsers = NodeToolOptionParser.class)
public class Starter {

  public static final String STARTED_MESSAGE = "Finished starting bundles.";

  protected static final String JAR_DIRECTORY =
      System.getProperty("stargate.libdir", "../stargate-lib");
  protected static final String CACHE_DIRECTORY = System.getProperty("stargate.bundle.cache.dir");

  @Inject protected HelpOption<Starter> help;

  @Order(value = 1)
  @Option(
      name = {"--cluster-name"},
      title = "cluster_name",
      arity = 1,
      description = "Name of backend cluster (required unless --nodetool is specified)")
  protected String clusterName;

  @Order(value = 2)
  @Option(
      name = {"--cluster-seed"},
      title = "seed_address",
      description = "Seed node address")
  protected List<String> seedList = new ArrayList<>();

  @Required
  @Order(value = 3)
  @Option(
      name = {"--cluster-version"},
      title = "version",
      description = "The major version number of the backend cluster (example: 3.11 or 4.0)")
  protected String version;

  @Order(value = 4)
  @Option(
      name = {"--listen"},
      title = "listen_ip",
      arity = 1,
      description = "address stargate services should listen on (default: 127.0.0.1)")
  protected String listenHostStr = "127.0.0.1";

  @Order(value = 5)
  @Option(
      name = {"--seed-port"},
      title = "port",
      arity = 1,
      description = "Port that seed nodes are listening on (default: 7000)")
  @Port
  protected Integer seedPort = 7000;

  @Order(value = 6)
  @Option(
      title = "datacenter_name",
      name = {"--dc"},
      arity = 1,
      description = "Datacenter name of this node (matching backend)")
  protected String dc;

  @Order(value = 7)
  @Option(
      title = "rack_name",
      name = {"--rack"},
      arity = 1,
      description = "Rack name of this node (matching backend)")
  protected String rack;

  @Order(value = 8)
  @Option(
      name = {"--simple-snitch"},
      description = "Set if backend uses the simple snitch")
  protected boolean simpleSnitch = false;

  @Order(value = 9)
  @Option(
      name = {"--dse"},
      description = "Set if backend is DSE, otherwise Cassandra")
  protected boolean dse = false;

  @Order(value = 10)
  @Option(
      name = {"--cql-port"},
      title = "port",
      description = "CQL Service port (default: 9042)")
  @Port
  protected int cqlPort = 9042;

  @Order(value = 11)
  @Option(
      name = {"--enable-auth"},
      description = "Set to enable PasswordAuthenticator")
  protected boolean enableAuth = false;

  @Order(value = 12)
  @Option(
      name = {"--use-proxy-protocol"},
      description = "Use proxy protocol to determine the public address and port of CQL requests")
  protected boolean useProxyProtocol = false;

  @Order(value = 13)
  @Option(
      name = {"--proxy-dns-name"},
      description =
          "Used with the proxy protocol flag to populate `system.peers` with a proxy's public IP addresses (i.e. A records)")
  protected String proxyDnsName;

  @Order(value = 14)
  @Option(
      name = {"--internal-proxy-dns-name"},
      description =
          "Used with the proxy protocol flag to populate `system.peers` with a proxy's private IP addresses (i.e. A records), "
              + "if the client connected through a private address. If not specified, it defaults to the value of `--proxy-dns-name` "
              + "prefixed with \"internal-\"")
  protected String internalProxyDnsName;

  @Order(value = 15)
  @Option(
      name = {"--proxy-port"},
      description =
          "Used with the proxy protocol flag to specify the proxy's listening port for the CQL protocol")
  protected int proxyPort = cqlPort;

  @Order(value = 16)
  @Option(
      name = {"--emulate-dbaas-defaults"},
      description =
          "Updated defaults reflect those of DataStax Astra at the time of the currently used DSE release")
  protected boolean emulateDbaasDefaults = false;

  @Order(value = 17)
  @Option(
      name = {"--developer-mode"},
      description =
          "Defines whether the stargate node should also behave as a "
              + "regular node, joining the ring with tokens assigned in order to facilitate getting started quickly and not "
              + "requiring additional nodes or existing cluster")
  protected boolean developerMode = false;

  @Order(value = 18)
  @Option(
      name = {"--bind-to-listen-address"},
      description = "When set, it binds web services to listen address only")
  protected boolean bindToListenAddressOnly = false;

  @Order(value = 19)
  @Option(
      name = {"--jmx-port"},
      description = "The port on which JMX should start")
  protected int jmxPort = 7199;

  @Order(value = 20)
  @Option(
      name = {
        "--disable-dynamic-snitch",
        "Whether the dynamic snitch should wrap the actual snitch."
      })
  protected boolean disableDynamicSnitch = false;

  @Order(value = 21)
  @Option(
      name = {"--disable-mbean-registration", "Whether the mbean registration should be disabled"})
  protected boolean disableMBeanRegistration = false;

  @Order(value = 22)
  @Option(
      name = {
        "--disable-bundles-watch",
        "Whether watching the bundle directory for new jars to load should be disabled"
      })
  protected boolean disableBundlesWatch = false;

  @Order(value = 22)
  @Option(
      name = {"--host-id"},
      description = "The host ID to use for this node. Must be a valid UUID.")
  protected String hostId;

  @Order(value = 24)
  @Option(
      name = "--bridge-token",
      description = "Token that API services will use to authenticate to the bridge (required) ")
  protected String bridgeToken;

  @Order(value = 1000)
  @Option(
      name = "--nodetool",
      description = "Run nodetool with all of the following arguments and exit")
  protected boolean nodetool;

  @Order(value = 1001)
  @Option(
      hidden = true,
      name = "-T",
      title = "arg",
      arity = 1,
      description = "Command line arguments for --nodetool (zero or more)")
  protected List<String> toolArgs = new ArrayList<>();

  private BundleContext context;
  private Felix framework;
  private List<Bundle> bundleList;
  private boolean watchBundles = true;
  private final AtomicBoolean startError = new AtomicBoolean();

  @Retention(RetentionPolicy.RUNTIME)
  public @interface Order {
    int value();
  }

  public Starter() {}

  // Visible for testing
  public Starter(
      String clusterName,
      String version,
      String listenHostStr,
      String seedHost,
      Integer seedPort,
      String dc,
      String rack,
      boolean dse,
      boolean isSimpleSnitch,
      int cqlPort,
      int jmxPort,
      String bridgeToken) {
    this.clusterName = clusterName;
    this.version = version;
    this.listenHostStr = listenHostStr;
    this.seedList = Collections.singletonList(seedHost);
    this.seedPort = seedPort;
    this.dc = dc;
    this.rack = rack;
    this.dse = dse;
    this.simpleSnitch = isSimpleSnitch;
    this.cqlPort = cqlPort;
    this.watchBundles = false;
    // bind to listen address only to allow multiple starters to start on the same host
    this.bindToListenAddressOnly = true;
    this.jmxPort = jmxPort;
    this.bridgeToken = bridgeToken;
    this.disableDynamicSnitch = true;
    this.disableMBeanRegistration = true;
  }

  protected void setStargateProperties() {
    if (version == null || version.trim().isEmpty() || !NumberUtils.isParsable(version)) {
      throw new IllegalArgumentException("--cluster-version must be a number");
    }

    if (clusterName == null || clusterName.trim().isEmpty()) {
      throw new IllegalArgumentException("--cluster-name must be specified");
    }

    if (bridgeToken == null || bridgeToken.trim().isEmpty()) {
      throw new IllegalArgumentException("--bridge-token must be specified");
    }

    if (!InetAddressValidator.getInstance().isValid(listenHostStr)) {
      throw new IllegalArgumentException("--listen must be a valid IPv4 or IPv6 address");
    }

    if (developerMode) {
      if (seedList.size() == 0) {
        // Default to use itself as seed in developer mode
        seedList.add(listenHostStr);
      }

      // Use a simple snitch for developer mode
      simpleSnitch = true;
    }

    if (!simpleSnitch && (dc == null || rack == null)) {
      throw new IllegalArgumentException(
          "--dc and --rack are both required unless --simple-snitch is specified.");
    }

    if (seedList.size() == 0) {
      throw new IllegalArgumentException("At least one seed node address is required.");
    }

    for (String seed : seedList) {
      try {
        InetAddress.getAllByName(seed.trim());
      } catch (UnknownHostException e) {
        throw new RuntimeException("Unable to resolve seed node address " + seed.trim());
      }
    }

    if (emulateDbaasDefaults && !dse) {
      throw new IllegalArgumentException(
          "--emulate-dbaas-defaults is currently only supported with DSE");
    }

    System.setProperty("stargate.persistence_id", dse ? "DsePersistence" : "CassandraPersistence");
    System.setProperty("stargate.datacenter", dc == null ? "datacenter1" : dc);
    System.setProperty("stargate.rack", rack == null ? "rack1" : rack);
    System.setProperty("stargate.cluster_name", clusterName);
    System.setProperty("stargate.seed_list", String.join(",", seedList));
    System.setProperty(
        "stargate.snitch_classname", simpleSnitch ? "SimpleSnitch" : "StargateConfigSnitch");
    System.setProperty("stargate.seed_port", String.valueOf(seedPort));
    System.setProperty("stargate.listen_address", listenHostStr);
    System.setProperty("stargate.cql_port", String.valueOf(cqlPort));
    System.setProperty("stargate.enable_auth", enableAuth ? "true" : "false");
    System.setProperty("stargate.use_proxy_protocol", useProxyProtocol ? "true" : "false");
    if (proxyDnsName != null) {
      System.setProperty("stargate.proxy_protocol.dns_name", proxyDnsName);
    }
    if (internalProxyDnsName != null) {
      System.setProperty("stargate.proxy_protocol.internal_dns_name", internalProxyDnsName);
    }
    System.setProperty("stargate.proxy_protocol.port", String.valueOf(proxyPort));
    System.setProperty("stargate.emulate_dbaas_defaults", emulateDbaasDefaults ? "true" : "false");
    System.setProperty("stargate.developer_mode", String.valueOf(developerMode));
    System.setProperty("stargate.bind_to_listen_address", String.valueOf(bindToListenAddressOnly));
    System.setProperty("cassandra.jmx.remote.port", String.valueOf(jmxPort));
    System.setProperty("cassandra.jmx.local.port", String.valueOf(jmxPort));
    System.setProperty("stargate.dynamic_snitch", String.valueOf(!disableDynamicSnitch));
    System.setProperty(
        "org.apache.cassandra.disable_mbean_registration",
        String.valueOf(disableMBeanRegistration));
    if (hostId != null && !hostId.isEmpty()) {
      System.setProperty("stargate.host_id", hostId);
    }
    System.setProperty("stargate.bridge.admin_token", bridgeToken);

    if (bindToListenAddressOnly) {
      // Restrict the listen address for Jersey endpoints
      System.setProperty("dw.server.connector.bindHost", listenHostStr);
    }

    // Don't step on native logback functionality. If someone wants to use built in logback args
    // then just use those.
    if (System.getProperty("logback.configurationFile") == null) {
      System.setProperty("logback.configurationFile", JAR_DIRECTORY + "/logback.xml");
    }
  }

  public Starter withAuthEnabled(boolean enabled) {
    enableAuth = enabled;
    return this;
  }

  public void start() throws BundleException {
    // Setup properties used by Stargate framework
    try {
      if (!nodetool) {
        setStargateProperties();
      }
    } catch (Exception e) {
      System.err.println(e.getMessage());
      System.exit(2);
    }

    // Initialize Apache Felix Framework
    framework = new Felix(felixConfig());
    framework.init();

    System.err.println("JAR DIR: " + JAR_DIRECTORY);

    // Install bundles
    context = framework.getBundleContext();
    context.addFrameworkListener(new BundleFailureListener());
    File[] files = new File(JAR_DIRECTORY).listFiles();
    List<File> jars = pickBundles(files);
    framework.start();

    bundleList = new ArrayList<>();
    // Install bundle JAR files and remember the bundle objects.
    for (File jar : jars) {
      System.out.println("Installing bundle " + jar.getName());
      Bundle b = context.installBundle(jar.toURI().toString());
      bundleList.add(b);
    }

    if (nodetool) {
      Bundle bundle = bundleList.get(0); // expect the persistence bundle to be first in the list
      try {
        Class<?> tool = bundle.loadClass("org.apache.cassandra.tools.NodeTool");

        System.out.println("Running NodeTool from " + bundle.getSymbolicName());

        Method main = tool.getMethod("main", String[].class);

        main.invoke(null, (Object) toolArgs.toArray(new String[0]));

        // exit just in case, NodeTool is expected to call System.exit() when done.
        System.exit(5);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    // Start all installed bundles.
    for (Bundle bundle : bundleList) {
      System.out.println("Starting bundle " + bundle.getSymbolicName());
      bundle.start();
    }

    if (startError.get()) {
      System.out.println("Terminating due to previous service startup errors.");
      System.exit(1);
    }

    System.out.println(STARTED_MESSAGE);

    if (watchBundles && !disableBundlesWatch) {
      watchJarDirectory(JAR_DIRECTORY);
    }
  }

  protected Map<String, String> felixConfig() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(Constants.FRAMEWORK_STORAGE_CLEAN, "onFirstInit");
    configMap.put(
        FelixConstants.LOG_LEVEL_PROP,
        System.getProperty("felix.log.level", String.valueOf(Logger.LOG_WARNING)));
    configMap.put(
        FelixConstants.FRAMEWORK_SYSTEMPACKAGES_EXTRA,
        String.join(
            ",", "sun.misc,sun.nio.ch", "com.sun.management", "sun.rmi.registry", "org.osgi.*"));
    configMap.put(
        FelixConstants.FRAMEWORK_BOOTDELEGATION,
        String.join(
            ",",
            "javax.*",
            "sun.*",
            "com.sun.*",
            "org.xml.sax",
            "org.xml.sax.*",
            "org.w3c.dom",
            "org.w3c.dom.*"));

    if (CACHE_DIRECTORY != null) {
      configMap.put(FelixConstants.FRAMEWORK_STORAGE, CACHE_DIRECTORY);
    }

    return configMap;
  }

  protected List<File> pickBundles(File[] files) {
    ArrayList<File> jars = new ArrayList<>();
    boolean foundVersion = false;

    if (files != null) {
      Arrays.sort(files);
      for (File file : files) {
        String name = file.getName().toLowerCase();

        // Avoid loading the wrong persistence module
        if (!dse
            && (name.contains("persistence-dse")
                || (name.contains("persistence-cassandra")
                    && !name.contains("persistence-cassandra-" + version)))) continue;

        if (dse
            && (name.contains("persistence-cassandra")
                || (name.contains("persistence-dse")
                    && !name.contains("persistence-dse-" + version)))) continue;

        if (name.contains("persistence-cassandra") || name.contains("persistence-dse")) {
          System.out.println("Loading persistence backend " + name);
          foundVersion = true;

          // Put the persistence bundle first in the list
          jars.add(0, file);
          continue;
        }

        if (name.endsWith(".jar") && !name.startsWith("stargate-starter-")) {
          jars.add(file);
        }
      }
    }

    if (!foundVersion)
      throw new IllegalArgumentException(
          String.format(
              "No persistence backend found for %s %s", (dse ? "dse" : "cassandra"), version));

    return jars;
  }

  public void stop() throws InterruptedException, BundleException {
    if (framework != null) {
      framework.stop();
      framework.waitForStop(TimeUnit.SECONDS.toMillis(30));
      this.context = null;
      this.framework = null;
    }
  }

  private void watchJarDirectory(String directoryPath) throws BundleException {
    try (WatchService watchService = FileSystems.getDefault().newWatchService()) {
      Path path = Paths.get(directoryPath);
      path.register(
          watchService,
          StandardWatchEventKinds.ENTRY_CREATE,
          StandardWatchEventKinds.ENTRY_DELETE,
          StandardWatchEventKinds.ENTRY_MODIFY);

      boolean poll = true;
      while (poll) {
        poll = pollEvents(watchService);
      }
    } catch (IOException | ClosedWatchServiceException | InterruptedException e) {
      // Since most deployments will occur in the cloud its not worth breaking the start up to
      // support hot-reload
      System.err.printf("Jars will not be watched due to unexpected error: %s%n", e.getMessage());
    }
  }

  private boolean pollEvents(WatchService watchService)
      throws InterruptedException, BundleException {
    WatchKey key = watchService.take();
    Path path = (Path) key.watchable();
    for (WatchEvent<?> event : key.pollEvents()) {
      processFileEvent(event.kind(), path.resolve((Path) event.context()).toFile());
    }
    return key.reset();
  }

  private void processFileEvent(WatchEvent.Kind<?> kind, File file) throws BundleException {
    if (!file.getName().toLowerCase().endsWith(".jar")
        || file.getName().toLowerCase().startsWith("stargate-starter-")) {
      return;
    }

    if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
      System.out.println("Event kind:" + kind + ". File affected: " + file.getAbsolutePath() + ".");
      Bundle b = context.installBundle(file.toURI().toString());
      System.out.println("Starting bundle " + b.getSymbolicName());
      b.start();
    } else if (kind == StandardWatchEventKinds.ENTRY_MODIFY) {
      System.out.println("Event kind:" + kind + ". File affected: " + file.getAbsolutePath() + ".");
      Bundle b = context.getBundle(file.toURI().toString());
      b.update();
    } else if (kind == StandardWatchEventKinds.ENTRY_DELETE) {
      System.out.println("Event kind:" + kind + ". File affected: " + file.getAbsolutePath() + ".");
      Bundle b = context.getBundle(file.toURI().toString());
      b.uninstall();
    }
  }

  public Optional<Object> getService(String className) throws InvalidSyntaxException {
    for (Bundle b : bundleList) {
      ServiceReference<Object> ref =
          (ServiceReference) b.getBundleContext().getServiceReference(className);
      if (ref != null) {
        Object t = ref.getBundle().getBundleContext().getService(ref);
        return Optional.of(t);
      }
    }

    return Optional.empty();
  }

  /** Prints help with options in the order specified in {@link Order} */
  protected static void printHelp(SingleCommand c) {
    try {
      new CliCommandUsageGenerator(
              79,
              (a, b) -> {
                Order aO = a.getAccessors().iterator().next().getAnnotation(Order.class);
                Order bO = b.getAccessors().iterator().next().getAnnotation(Order.class);

                if (aO == null && bO == null) return 0;

                if (aO == null) return 1;

                if (bO == null) return -1;

                return aO.value() - bO.value();
              },
              false)
          .usage(c.getCommandMetadata(), c.getParserConfiguration(), System.err);
    } catch (IOException e) {
      throw new IOError(e);
    }
  }

  protected static void cli(String[] args, Class<? extends Starter> starterClass) {
    SingleCommand<? extends Starter> parser = SingleCommand.singleCommand(starterClass);
    try {
      ParseResult<? extends Starter> result = parser.parseWithResult(args);

      if (result.wasSuccessful()) {
        // Parsed successfully, so just run the command and exit
        result.getCommand().start();
      } else {

        printHelp(parser);
        System.err.println();

        // Parsing failed
        // Display errors and then the help information
        System.err.printf("%d errors encountered:%n", result.getErrors().size());
        int i = 1;
        for (ParseException e : result.getErrors()) {
          System.err.printf("Error %d: %s%n", i, e.getMessage());
          i++;
        }

        System.err.println();
      }
    } catch (ParseException p) {
      printHelp(parser);

      // noinspection StatementWithEmptyBody
      if (args.length > 0 && (args[0].equals("-h") || args[0].equals("--help"))) {
        // don't tell the user it's a usage error
        // a bug in airline (https://github.com/airlift/airline/issues/44) prints a usage error even
        // if
        // a user just uses -h/--help
      } else {
        System.err.println();
        System.err.printf("Usage error: %s%n", p.getMessage());
        System.err.println();
      }
    } catch (Exception e) {
      // Errors should be being collected so if anything is thrown it is unexpected
      System.err.printf("Unexpected error: %s%n", e.getMessage());
      e.printStackTrace(System.err);

      System.exit(1);
    }
  }

  public static void main(String[] args) {
    cli(args, Starter.class);
  }

  private class BundleFailureListener implements FrameworkListener {

    @Override
    public void frameworkEvent(FrameworkEvent event) {
      Throwable throwable = event.getThrowable();
      if (throwable != null) {
        // We rely on the exception class name here because there is no easy way to share
        // classes between the Starter and bundles, and this use case does not seem worth
        // adding a new OSGi artifact.
        if (throwable.getClass().getSimpleName().equals("ServiceStartException")) {
          startError.set(true);

          System.out.printf(
              "Detected service startup failure in bundle %s: %s%n",
              event.getBundle().getSymbolicName(), throwable);

          // This should be logged by OSGi, but just in case dump it to STDERR too
          throwable.printStackTrace(System.err);
        }
      }
    }
  }

  public static class NodeToolOptionParser<T> extends AbstractOptionParser<T> {

    @Override
    public ParseState<T> parseOptions(
        PeekingIterator<String> tokens, ParseState<T> state, List<OptionMetadata> allowedOptions) {
      OptionMetadata nodetoolOption = findOption(state, allowedOptions, "--nodetool");

      String token = tokens.peek();
      if (!nodetoolOption.getOptions().contains(token)) {
        return null; // fallback to default parsers
      }

      OptionMetadata argsOption = findOption(state, allowedOptions, "-T");

      // drop the peeked token
      tokens.next();
      state = state.withOptionValue(nodetoolOption, "true");

      // treat remaining tokens as NodeTool args
      while (tokens.hasNext()) {
        state = state.withOptionValue(argsOption, tokens.next());
      }

      return state;
    }
  }
}
