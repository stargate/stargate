package io.stargate.starter;

import static org.slf4j.LoggerFactory.getLogger;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.Startup;
import io.quarkus.runtime.annotations.CommandLineArguments;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.spi.ObserverMethod;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import picocli.CommandLine;

/**
 * This is a starter that process the {@link StarctlCommand} manually and exits the application if
 * the command fails or help is requested.
 *
 * <p>This @Startup must have lower priority than any other @Startup that may be present in the
 * classpath.
 */
@Dependent
@Startup(ObserverMethod.DEFAULT_PRIORITY - 400)
public class StargateStarter {

  private static final Logger logger = getLogger(StargateStarter.class);

  @Inject @CommandLineArguments String[] args;

  @Inject StarctlCommand command;

  @PostConstruct
  void startup() {
    CommandLine commandLine = new CommandLine(command);
    int result = commandLine.execute(args);
    if (result != 0) {
      logger.error("Failed to start Stargate, exiting..");
      Quarkus.asyncExit(result);
    }

    if (commandLine.isUsageHelpRequested() || commandLine.isVersionHelpRequested()) {
      logger.info("Command help requested, exiting..");
      Quarkus.asyncExit(0);
    }
  }
}
