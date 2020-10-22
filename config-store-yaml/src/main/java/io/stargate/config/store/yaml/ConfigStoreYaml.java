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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.stargate.config.store.ConfigStore;
import io.stargate.config.store.MissingExtensionSettingsException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class ConfigStoreYaml implements ConfigStore {
  private final ObjectMapper mapper;
  private final Path configFilePath;
  private final MapType yamlConfigType;

  public ConfigStoreYaml(Path configFilePath) {
    this.configFilePath = configFilePath;
    mapper = new ObjectMapper(new YAMLFactory());
    MapType mapType =
        mapper.getTypeFactory().constructMapType(HashMap.class, String.class, Object.class);
    yamlConfigType =
        mapper
            .getTypeFactory()
            .constructMapType(
                HashMap.class, mapper.getTypeFactory().constructType(String.class), mapType);
  }

  @Override
  public Map<String, Object> getConfigForExtension(String extensionName)
      throws MissingExtensionSettingsException {
    try {
      Map<String, Map<String, Object>> result =
          mapper.readValue(configFilePath.toFile(), yamlConfigType);
      if (!result.containsKey(extensionName)) {
        throw new MissingExtensionSettingsException(
            String.format(
                "The loaded configuration map: %s, does not contain settings from a given extension: %s",
                result, extensionName));
      }
      return result.get(extensionName);
    } catch (IOException e) {
      throw new YamlConfigException("Problem when processing yaml file from: " + configFilePath, e);
    }
  }
}
