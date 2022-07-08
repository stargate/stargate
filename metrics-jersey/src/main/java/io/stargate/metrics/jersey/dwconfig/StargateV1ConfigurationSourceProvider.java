package io.stargate.metrics.jersey.dwconfig;

import io.dropwizard.configuration.ConfigurationSourceProvider;
import io.dropwizard.configuration.FileConfigurationSourceProvider;
import io.dropwizard.configuration.ResourceConfigurationSourceProvider;
import java.io.ByteArrayInputStream;
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
 *   <li>File "${CONFIG_NAME} (like {@code restapi-config.yaml} -- same name as the default config
 *       resource -- in the Current Working Directory: if found, will be used
 *   <li>Classpath Resource with name "${CONFIG_NAME}"
 * </ol>
 *
 * (where "${MODULENAME}" and "${CONFIG_NAME} are passed as arguments; former typically being
 * something like {@code "restapi"} (former) and {@code "restapi-config.yaml"} (latter))
 */
public class StargateV1ConfigurationSourceProvider implements ConfigurationSourceProvider {
  public static final String SYSPROP_CONFIG_FILE_PREFIX = "stargate.configurationFile.";

  private final String moduleName;

  public StargateV1ConfigurationSourceProvider(String moduleName) {
    this.moduleName = Objects.requireNonNull(moduleName);
  }

  /**
   * @param configName Name of the config file or resource to look for: typically starts with module
   *     name followed by {@code "-config.yaml"} (so for example {@code "restapi-config.yaml"})
   * @return InputStream for reading the configuration file/resource to use
   * @throws IOException In case opening of the resource failed
   */
  public InputStream open(String configName) throws IOException {
    // First: do we have system property?
    String sysprop = SYSPROP_CONFIG_FILE_PREFIX + moduleName;
    String customConfigFile = System.getProperty(sysprop);
    if (customConfigFile == null) {
      log("No value for configuration override System Property '%s'", sysprop, customConfigFile);
    } else {
      log(
          "Found configuration override System Property '%s'; will use config file '%s'",
          sysprop, customConfigFile);
      return new FileConfigurationSourceProvider().open(customConfigFile);
    }
    // Second: actual File in CWD with config file name
    File f = new File(configName).getAbsoluteFile();
    if (!f.exists()) {
      log("No configuration override file '%s' found", f);
    } else {
      log("Found configuration override file '%s', will use that", f);
      return new FileConfigurationSourceProvider().open(f.getPath());
    }
    // And finally if neither found, read the default resource
    log("No configuration overrides found, will use the default config resource '%s'", configName);
    InputStream in = new ResourceConfigurationSourceProvider().open(configName);
    // Avoid NPE by caller by returning empty Stream
    return (in == null) ? new ByteArrayInputStream(new byte[0]) : in;
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
