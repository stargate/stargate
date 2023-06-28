package io.stargate.starter;

import static org.slf4j.LoggerFactory.getLogger;

import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import java.util.List;
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
      description = "Seed node address")
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

  @Override
  public void run() {
    logger.info("Cluster name set to {}", clusterName);
    System.setProperty("stargate.cluster_name", clusterName);
  }
}
