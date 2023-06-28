package io.stargate.starter;

import static org.slf4j.LoggerFactory.getLogger;

import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import org.apache.commons.validator.routines.InetAddressValidator;
import org.slf4j.Logger;
import picocli.CommandLine;

@Dependent
@CommandLine.Command(name = "starctl", mixinStandardHelpOptions = true)
public class StarctlCommand implements Runnable {

  private static final Logger logger = getLogger(StarctlCommand.class);

  @Inject CommandLine.IFactory factory;

  @CommandLine.Option(
      names = {"--cluster-name"},
      description = "Name of backend cluster (required unless --nodetool is specified)",
      required = true)
  String clusterName;

  @CommandLine.Option(
      names = {"--cluster-seed"},
      description = "Seed node address",
      required = true)
  List<String> seedList;

  @CommandLine.Option(
      names = {"--listen"},
      description = "Address stargate services should listen on (default: 127.0.0.1)",
      defaultValue = "127.0.0.1")
  String listenHostStr;

  @CommandLine.Option(
      names = {"--seed-port"},
      description = "Port that seed nodes are listening on (default: 7000)",
      defaultValue = "7000")
  Integer seedPort;

  @CommandLine.Option(
      names = {"--simple-snitch"},
      description = "Set if backend uses the simple snitch (default: false)",
      defaultValue = "false")
  boolean simpleSnitch = false;

  @CommandLine.Option(
      names = {"--bind-to-listen-address"},
      description = "When set, it binds web services to listen address only (default: false)",
      defaultValue = "false")
  boolean bindToListenAddressOnly = false;

  @Override
  public void run() {
    if (!InetAddressValidator.getInstance().isValid(listenHostStr)) {
      throw new IllegalArgumentException("--listen must be a valid IPv4 or IPv6 address");
    }

    for (String seed : seedList) {
      try {
        InetAddress.getAllByName(seed.trim());
      } catch (UnknownHostException e) {
        throw new IllegalArgumentException("Unable to resolve seed node address " + seed.trim());
      }
    }

    logger.info("Cluster name set to {}", clusterName);
    System.setProperty("stargate.cluster_name", clusterName);

    String seeds = String.join(",", seedList);
    logger.info("Seed node addresses set to {}", seedList);
    System.setProperty("stargate.seed_list", seeds);

    logger.info("Seed port set to {}", seedPort);
    System.setProperty("stargate.seed_port", String.valueOf(seedPort));

    String snitchClass = simpleSnitch ? "SimpleSnitch" : "StargateConfigSnitch";
    logger.info("Using snitch class {}", snitchClass);
    System.setProperty("stargate.snitch_classname", snitchClass);

    logger.info("Using listen address {}", listenHostStr);
    System.setProperty("stargate.listen_address", listenHostStr);

    if (bindToListenAddressOnly) {
      logger.info("Binding to listen address only");
    }
    System.setProperty("stargate.bind_to_listen_address", String.valueOf(bindToListenAddressOnly));
  }
}
