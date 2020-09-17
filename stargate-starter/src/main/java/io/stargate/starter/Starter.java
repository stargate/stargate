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
import com.github.rvesse.airline.annotations.help.License;
import com.github.rvesse.airline.annotations.restrictions.Pattern;
import com.github.rvesse.airline.annotations.restrictions.Port;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.github.rvesse.airline.help.cli.CliCommandUsageGenerator;
import com.github.rvesse.airline.parser.ParseResult;
import com.github.rvesse.airline.parser.errors.ParseException;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
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
import javax.inject.Inject;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.felix.framework.Felix;
import org.apache.felix.framework.util.FelixConstants;
import org.apache.felix.resolver.Logger;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleException;
import org.osgi.framework.Constants;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceReference;

/** Starts a OSGi container and installs and starts all needed bundles */
@License(url = "https://www.apache.org/licenses/LICENSE-2.0")
@Command(name = "Stargate")
public class Starter {

  private static final String JAR_DIRECTORY =
      System.getProperty("stargate.libdir", "../stargate-lib");

  @Retention(RetentionPolicy.RUNTIME)
  public @interface Order {
    int value();
  }

  @Inject HelpOption<Starter> help;

  @Required
  @Order(value = 1)
  @Option(
      name = {"--cluster-name"},
      title = "cluster_name",
      arity = 1,
      description = "Name of backend cluster")
  String clusterName;

  @Order(value = 2)
  @Option(
      name = {"--cluster-seed"},
      title = "seed_address",
      description = "Seed node address")
  List<String> seedList = new ArrayList<>();

  @Required
  @Order(value = 3)
  @Option(
      name = {"--cluster-version"},
      title = "version",
      description = "The major version number of the backend cluster (example: 3.11 or 4.0)")
  String version;

  @Order(value = 4)
  @Option(
      name = {"--listen"},
      title = "listen_ip",
      arity = 1,
      description = "address stargate services should listen on (default: 127.0.0.1)")
  @Pattern(
      pattern = "^[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+$",
      description = "Must be a valid IP address")
  String listenHostStr = "127.0.0.1";

  @Order(value = 5)
  @Option(
      name = {"--seed-port"},
      title = "port",
      arity = 1,
      description = "Port that seed nodes are listening on (default: 7000)")
  @Port
  Integer seedPort = 7000;

  @Order(value = 6)
  @Option(
      title = "datacenter_name",
      name = {"--dc"},
      arity = 1,
      description = "Datacenter name of this node (matching backend)")
  String dc;

  @Order(value = 7)
  @Option(
      title = "rack_name",
      name = {"--rack"},
      arity = 1,
      description = "Rack name of this node (matching backend)")
  String rack;

  @Order(value = 8)
  @Option(
      name = {"--simple-snitch"},
      description = "Set if backend uses the simple snitch")
  boolean simpleSnitch = false;

  @Order(value = 9)
  @Option(
      name = {"--dse"},
      description = "Set if backend is DSE, otherwise Cassandra")
  boolean dse = false;

  @Order(value = 10)
  @Option(
      name = {"--cql-port"},
      title = "port",
      description = "CQL Service port (default: 9042)")
  @Port
  int cqlPort = 9042;

  @Order(value = 11)
  @Option(
      name = {"--enable-auth"},
      description = "Set to enable PasswordAuthenticator")
  private boolean enableAuth = false;

  @Order(value = 12)
  @Option(
      name = {"--use-proxy-protocol"},
      description = "Use proxy protocol to determine the public address and port of CQL requests")
  private boolean useProxyProtocol = false;

  @Order(value = 13)
  @Option(
      name = {"--emulate-dbaas-defaults"},
      description =
          "Updated defaults reflect those of DataStax Astra at the time of the currently used DSE release")
  private boolean emulateDbaasDefaults = false;

  @Order(value = 14)
  @Option(
      name = {"--developer-mode"},
      description =
          "Defines whether the stargate node should also behave as a "
              + "regular node, joining the ring with tokens assigned in order to facilitate getting started quickly and not "
              + "requiring additional nodes or existing cluster")
  boolean developerMode = false;

  @Order(value = 15)
  @Option(
      name = {"--bind-to-listen-address"},
      description = "When set, it binds web services to listen address only")
  boolean bindToListenAddressOnly = false;

  @Order(value = 16)
  @Option(
      name = {"-jmx-port"},
      description = "The port on which JMX should start")
  int jmxPort = 7199;

  @Order(value = 17)
  @Option(name = {"--dynamic_snitch", "Set if the dynamic snitch should wrap the actual snitch."})
  boolean dynamicSnitch = true;

  private BundleContext context;
  private Felix framework;
  private List<Bundle> bundleList;
  private boolean watchBundles = true;

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
      int jmxPort) {
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
    this.dynamicSnitch = false;
  }

  void setStargateProperties() {
    if (version == null || version.trim().isEmpty() || !NumberUtils.isParsable(version)) {
      throw new IllegalArgumentException("--cluster-version must be a number");
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
    System.setProperty("stargate.emulate_dbaas_defaults", emulateDbaasDefaults ? "true" : "false");
    System.setProperty("stargate.developer_mode", String.valueOf(developerMode));
    System.setProperty("stargate.bind_to_listen_address", String.valueOf(bindToListenAddressOnly));
    System.setProperty("cassandra.jmx.remote.port", String.valueOf(jmxPort));
    System.setProperty("cassandra.jmx.local.port", String.valueOf(jmxPort));
    System.setProperty("stargate.dynamic_snitch", String.valueOf(dynamicSnitch));

    if (bindToListenAddressOnly) {
      // Restrict the listen address for Jersey endpoints
      System.setProperty("dw.server.adminConnectors[0].bindHost", listenHostStr);
      System.setProperty("dw.server.applicationConnectors[0].bindHost", listenHostStr);
    }

    // Don't step on native logback functionality. If someone wants to use built in logback args
    // then just use those.
    if (System.getProperty("logback.configurationFile") == null) {
      System.setProperty("logback.configurationFile", JAR_DIRECTORY + "/logback.xml");
    }
  }

  public void start() throws BundleException {
    // Setup properties used by Stargate framework
    try {
      setStargateProperties();
    } catch (Exception e) {
      System.err.println(e.getMessage());
      System.exit(2);
    }

    // Initialize Apache Felix Framework
    Map<String, String> configMap = new HashMap<>();
    configMap.put(Constants.FRAMEWORK_STORAGE_CLEAN, "onFirstInit");
    configMap.put(
        FelixConstants.LOG_LEVEL_PROP,
        System.getProperty("felix.log.level", String.valueOf(Logger.LOG_WARNING)));
    configMap.put(
        FelixConstants.FRAMEWORK_SYSTEMPACKAGES_EXTRA, "sun.misc,sun.nio.ch,com.sun.management");
    framework = new Felix(configMap);
    framework.init();

    System.err.println("JAR DIR: " + JAR_DIRECTORY);

    // Install bundles
    context = framework.getBundleContext();
    File[] files = new File(JAR_DIRECTORY).listFiles();
    ArrayList<File> jars = new ArrayList<>();
    boolean foundVersion = false;

    if (files != null) {
      Arrays.sort(files);
      for (File file : files) {
        String name = file.getName().toLowerCase();

        // Avoid loading the wrong persistance module
        if (!dse
            && (name.contains("persistence-dse")
                || name.contains("persistence-cassandra")
                    && !name.contains("persistence-cassandra-" + version))) continue;

        if (dse
            && (name.contains("persistence-cassandra")
                || (name.contains("persistence-dse")
                    && !name.contains("persistence-dse-" + version)))) continue;

        if (name.contains("persistence-cassandra") || name.contains("persistence-dse")) {
          System.out.println("Loading persistence backend " + name);
          foundVersion = true;
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

    framework.start();

    bundleList = new ArrayList<>();
    // Install bundle JAR files and remember the bundle objects.
    for (File jar : jars) {
      System.out.println("Installing bundle " + jar.getName());
      Bundle b = context.installBundle(jar.toURI().toString());
      bundleList.add(b);
    }
    // Start all installed bundles.
    for (Bundle bundle : bundleList) {
      System.out.println("Starting bundle " + bundle.getSymbolicName());
      bundle.start();
    }

    if (watchBundles) watchJarDirectory(JAR_DIRECTORY);
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
      throw new RuntimeException(e);
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
  private static void printHelp(SingleCommand c) {
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

  public static void main(String[] args) {
    SingleCommand<Starter> parser = SingleCommand.singleCommand(Starter.class);
    try {
      ParseResult<Starter> result = parser.parseWithResult(args);

      if (result.wasSuccessful()) {
        // Parsed successfully, so just run the command and exit
        result.getCommand().start();
      } else {

        printHelp(parser);
        System.err.println();

        // Parsing failed
        // Display errors and then the help information
        System.err.println(String.format("%d errors encountered:", result.getErrors().size()));
        int i = 1;
        for (ParseException e : result.getErrors()) {
          System.err.println(String.format("Error %d: %s", i, e.getMessage()));
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
        System.err.println(String.format("Usage error: %s", p.getMessage()));
        System.err.println();
      }
    } catch (Exception e) {
      // Errors should be being collected so if anything is thrown it is unexpected
      System.err.println(String.format("Unexpected error: %s", e.getMessage()));
      e.printStackTrace(System.err);

      System.exit(1);
    }
  }
}
