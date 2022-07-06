package io.stargate.metrics.jersey.dwconfig;

import io.dropwizard.configuration.ConfigurationSourceProvider;
import io.dropwizard.configuration.FileConfigurationSourceProvider;
import io.dropwizard.configuration.ResourceConfigurationSourceProvider;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/**
 * Implementation of {@link ConfigurationSourceProvider} which will try to locate configuration file
 * using 3 possibilities, in descending order of precedence
 *
 * <ol>
 *   <li>Explicit System Property of form "stargate.configurationFile.${MODULENAME}" specifying
 *       exact filename of the configuration file to use
 *   <li>Existence of file "${MODULENAME}-${CONFIGBASE} which is read as the configuration
 *   <li>Classpath Resource with name "${CONFIGBASE}"
 * </ol>
 *
 * (where "${MODULENAME}" and "${CONFIGBASE} are passed as arguments; former typically being
 * something like {@code "restapi"} and latter {@code "config.yaml"})
 */
public class StargateV1ConfigurationSourceProvider implements ConfigurationSourceProvider {
  public static final String SYSPROP_CONFIG_FILE_PREFIX = "stargate.configurationFile.";

  private final String moduleName;

  public StargateV1ConfigurationSourceProvider(String moduleName) {
    this.moduleName = Objects.requireNonNull(moduleName);
  }

  public InputStream open(String path) throws IOException {
    // First: do we have system property?
    String sysprop = SYSPROP_CONFIG_FILE_PREFIX + moduleName;
    String configFilename = System.getProperty(sysprop);
    if (configFilename == null) {
      log("No value for configuration override System Property '%s'", sysprop, configFilename);
    } else {
      log(
          "Found configuration override System Property '%s'; will use config file '%s'",
          sysprop, configFilename);
      return new FileConfigurationSourceProvider().open(configFilename);
    }
    configFilename = String.format("%s-%s", moduleName, path);
    File f = new File(configFilename).getAbsoluteFile();
    if (!f.exists()) {
      log("No configuration override file '%s' found", f);
    } else {
      log("Found configuration override file '%s', will use that", f);
      return new FileConfigurationSourceProvider().open(configFilename);
    }
    log("No configuration overrides found, will use the default config resource '%s'", path);
    return new ResourceConfigurationSourceProvider().open(path);
  }

  private void log(String format, Object... args) {
    String message = String.format(format, args);
    message =
        String.format(
            "INFO [StargateV1ConfigurationSourceProvider](module=%s): %s", moduleName, message);
    // NOTE: this is during bootstrapping so cannot actually use slf4j, unfortunately
    System.err.println(message);
  }
}
