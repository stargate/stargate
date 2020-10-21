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
package io.stargate.config.store.k8s.config.map;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.DoneableConfigMap;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.stargate.config.store.ConfigStore;
import java.io.IOException;
import java.util.Map;

public class ConfigStoreK8sConfigMap implements ConfigStore {
  private final NamespacedKubernetesClient client;
  private final String namespace;

  public ConfigStoreK8sConfigMap(String namespace, NamespacedKubernetesClient client) {
    this.namespace = namespace;
    this.client = client;
  }

  @Override
  public Map<String, String> getConfigForExtension(String extensionName) {
    Resource<ConfigMap, DoneableConfigMap> configMaps =
        client.configMaps().inNamespace(namespace).withName(extensionName);
    return configMaps.get().getData();
  }

  @Override
  public void addOrReplaceConfigValueForExtension(String extensionName, String key, String value) {
    ConfigMap configMap = client.configMaps().inNamespace(namespace).withName(extensionName).get();
    if (configMap == null) {
      // create new config map
      ConfigMap newConfigMap =
          new ConfigMapBuilder()
              .withNewMetadata()
              .withName(extensionName)
              .endMetadata()
              .addToData(key, value)
              .build();
      client.configMaps().inNamespace(namespace).create(newConfigMap);
    } else {
      // create a config map based on the existing one
      ConfigMap newConfigMap =
          new ConfigMapBuilder()
              .withNewMetadata()
              .withName(extensionName)
              .endMetadata()
              .addToData(configMap.getData())
              .addToData(key, value)
              .build();
      client.configMaps().inNamespace(namespace).create(newConfigMap);
    }
  }

  @Override
  public void close() throws IOException {
    client.close();
  }
}
