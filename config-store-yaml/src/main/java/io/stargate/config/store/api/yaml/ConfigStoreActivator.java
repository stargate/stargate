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
package io.stargate.config.store.api.yaml;

import io.stargate.config.store.api.ConfigStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Hashtable;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigStoreActivator implements BundleActivator {

  private static final Logger logger = LoggerFactory.getLogger(ConfigStoreActivator.class);

  public static final String CONFIG_STORE_YAML_IDENTIFIER = "ConfigStoreYaml";

  private boolean configStoreEnabled;

  private final String configYamlLocation;

  // for testing purpose
  public ConfigStoreActivator(boolean configStoreEnabled, String configYamlLocation) {
    this.configStoreEnabled = configStoreEnabled;
    this.configYamlLocation = configYamlLocation;
  }

  public ConfigStoreActivator() {
    this(
        Boolean.parseBoolean(System.getProperty("stargate.config_store.enabled", "false")),
        System.getProperty(
            "stargate.config_store.yaml.location", "/etc/stargate/stargate-config.yaml"));
  }

  @Override
  public void start(BundleContext context) {
    if (!configStoreEnabled) {
      logger.info("Config Store YAML disabled - it will not be started");
      return;
    }
    logger.info("Starting Config Store YAML...");
    Path yamlFilePath = Paths.get(configYamlLocation);
    if (!Files.exists(yamlFilePath)) {
      throw new IllegalArgumentException(
          String.format(
              "The yaml file does not exists, please check the path: %s. The ConfigStoreYaml will not be registered.",
              yamlFilePath));
    }

    Hashtable<String, String> props = new Hashtable<>();
    props.put("Identifier", CONFIG_STORE_YAML_IDENTIFIER);

    ConfigStoreYaml configStoreYaml = new ConfigStoreYaml(yamlFilePath);
    context.registerService(ConfigStore.class, configStoreYaml, props);
    logger.info("Started Config Store YAML....");
  }

  @Override
  public void stop(BundleContext context) {
    // no-op
  }
}
