package io.stargate.app;

import static org.slf4j.LoggerFactory.getLogger;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import picocli.CommandLine;

@QuarkusMain
@CommandLine.Command(name = "starctl", mixinStandardHelpOptions = true)
public class App implements Runnable, QuarkusApplication {

  private static Logger logger = getLogger(App.class);

  @Inject CommandLine.IFactory factory;

  @CommandLine.Option(
      names = {"--cluster-name"},
      description = "Name of backend cluster (required unless --nodetool is specified)",
      required = true)
  String clusterName;

  @Override
  public void run() {
    logger.info("Cluster name set to {}", clusterName);
    System.setProperty("stargate.cluster_name", clusterName);
  }

  @Override
  public int run(String... args) throws Exception {
    int result = new CommandLine(this, factory).execute(args);
    logger.info("command result {}", result);

    Quarkus.waitForExit();
    return 0;
  }

  public static void main(String[] args) {
    Quarkus.run(App.class, args);
  }
}
