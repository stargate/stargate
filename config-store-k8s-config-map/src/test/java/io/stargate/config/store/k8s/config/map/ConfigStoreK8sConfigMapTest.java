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

import static org.assertj.core.api.Assertions.assertThat;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import io.stargate.config.store.ConfigStore;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ConfigStoreK8sConfigMapTest {
  private static KubernetesServer server;

  @BeforeAll
  public static void setup() {
    server = new KubernetesServer(true, true);
    server.before();
  }

  @AfterAll
  public static void cleanup() {
    server.after();
  }

  @Test
  public void shouldGetSettingFromConfigMap() {
    // given
    NamespacedKubernetesClient client = server.getClient();
    String namespace = "ns1";
    String firstExtensionName = generateExtensionName();
    String secondExtensionName = generateExtensionName();
    // create config maps
    ConfigMap configMap1 =
        new ConfigMapBuilder()
            .withNewMetadata()
            .withName(firstExtensionName)
            .endMetadata()
            .addToData("key", "value")
            .build();
    ConfigMap configMap2 =
        new ConfigMapBuilder()
            .withNewMetadata()
            .withName(secondExtensionName)
            .addToLabels("foo", "bar")
            .endMetadata()
            .addToData("other-setting", "value_2")
            .build();

    client.configMaps().inNamespace(namespace).create(configMap1);
    client.configMaps().inNamespace(namespace).create(configMap2);

    // when
    ConfigStore configStore =
        new ConfigStoreBuilder()
            .withNamespace(namespace)
            .withConfig(client.getConfiguration())
            .build();

    // then
    Map<String, String> configForExtension = configStore.getConfigForExtension(firstExtensionName);
    assertThat(configForExtension).containsOnly(new SimpleEntry<>("key", "value"));

    Map<String, String> configForExtension2 =
        configStore.getConfigForExtension(secondExtensionName);
    assertThat(configForExtension2).containsOnly(new SimpleEntry<>("other-setting", "value_2"));
  }

  @Test
  public void shouldAddSettingToNonExistingConfigMapProgrammatically() {
    // given
    NamespacedKubernetesClient client = server.getClient();
    String namespace = "ns1";
    ConfigStore configStore =
        new ConfigStoreBuilder()
            .withNamespace(namespace)
            .withConfig(client.getConfiguration())
            .build();
    String extensionName = generateExtensionName();

    // when add setting
    configStore.addOrReplaceConfigValueForExtension(extensionName, "key", "value");

    // then should retrieve the setting
    Map<String, String> configForExtension = configStore.getConfigForExtension(extensionName);
    assertThat(configForExtension).containsOnly(new SimpleEntry<>("key", "value"));
  }

  @Test
  public void shouldAddSettingToExistingConfigMapProgrammatically() {
    // given
    NamespacedKubernetesClient client = server.getClient();
    String namespace = "ns1";
    ConfigStore configStore =
        new ConfigStoreBuilder()
            .withNamespace(namespace)
            .withConfig(client.getConfiguration())
            .build();
    String extensionName = generateExtensionName();

    // when add setting
    configStore.addOrReplaceConfigValueForExtension(extensionName, "key", "value");
    configStore.addOrReplaceConfigValueForExtension(extensionName, "key", "value_override");
    configStore.addOrReplaceConfigValueForExtension(extensionName, "key_2", "value");

    // then should retrieve the setting
    Map<String, String> configForExtension = configStore.getConfigForExtension(extensionName);
    assertThat(configForExtension)
        .containsOnly(
            new SimpleEntry<>("key", "value_override"), new SimpleEntry<>("key_2", "value"));
  }

  private String generateExtensionName() {
    return UUID.randomUUID().toString();
  }
}
