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

import io.stargate.config.store.api.ConfigStore;
import io.stargate.core.activator.BaseActivator;
import io.stargate.core.metrics.api.Metrics;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;

public class ConfigStoreActivator extends BaseActivator {

  public static final String CONFIG_STORE_YAML_METRICS_PREFIX = "config.store.yaml";
  public static final String CONFIG_STORE_YAML_IDENTIFIER = "ConfigStoreYaml";

  private final String configYamlLocation;

  // for testing purpose
  public ConfigStoreActivator(String configYamlLocation) {
    super("Config Store YAML", Collections.singletonList(Metrics.class), ConfigStore.class);
    this.configYamlLocation = configYamlLocation;
  }

  public ConfigStoreActivator() {
    this(
        System.getProperty(
            "stargate.config_store.yaml.location", "/etc/stargate/stargate-config.yaml"));
  }

  @Override
  protected ServiceAndProperties createService(List<Object> dependentServices) {
    Metrics metrics = (Metrics) dependentServices.get(0);

    Hashtable<String, String> props = new Hashtable<>();
    props.put("Identifier", CONFIG_STORE_YAML_IDENTIFIER);

    return new ServiceAndProperties(
        new ConfigStoreYaml(
            Paths.get(configYamlLocation), metrics.getRegistry(CONFIG_STORE_YAML_METRICS_PREFIX)),
        props);
  }
}
