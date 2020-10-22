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
package io.stargate.config.store.yaml;

import io.stargate.config.store.ConfigStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Hashtable;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceEvent;
import org.osgi.framework.ServiceListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigStoreActivator implements BundleActivator, ServiceListener {
  private static final Logger logger = LoggerFactory.getLogger(ConfigStoreActivator.class);

  private static final String CONFIG_YAML_LOCATION =
      System.getProperty("stargate.config_store.yaml.location", "/etc/config/stargate-config.yaml");

  @Override
  public void start(BundleContext context) {
    logger.info("Starting Config Store YAML...");
    Path yamlFilePath = Paths.get(CONFIG_YAML_LOCATION);
    if (!Files.exists(yamlFilePath)) {
      logger.error(
          "The yaml file does not exists, please check the path: {}. The ConfigStoreYaml will not be registered.",
          yamlFilePath);
      return;
    }
    ConfigStoreYaml configStoreYaml = new ConfigStoreYaml(yamlFilePath);
    context.registerService(ConfigStore.class, configStoreYaml, new Hashtable<>());
    logger.info("Started Config Store YAML....");
  }

  @Override
  public void stop(BundleContext context) {
    // no-op
  }

  @Override
  public void serviceChanged(ServiceEvent event) {
    // no-op because ConfigStoreYaml does not depend on any other service
  }
}
