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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.stargate.config.store.MissingExtensionSettingsException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.Objects;
import org.junit.jupiter.api.Test;

class ConfigStoreYamlTest {

  @Test
  public void shouldGetSettingFromConfigYaml() {
    // given
    ClassLoader classLoader = getClass().getClassLoader();
    Path path =
        Paths.get(
            Objects.requireNonNull(classLoader.getResource("stargate-config.yaml")).getPath());
    ConfigStoreYaml configStoreYaml = new ConfigStoreYaml(path);

    // when
    Map<String, Object> configForExtension1 = configStoreYaml.getConfigForExtension("extension-1");
    Map<String, Object> configForExtension2 = configStoreYaml.getConfigForExtension("extension-2");

    // then
    assertThat(configForExtension1)
        .containsOnly(new SimpleEntry<>("a", 1), new SimpleEntry<>("b", "value"));
    assertThat(configForExtension2)
        .containsOnly(new SimpleEntry<>("a", 2), new SimpleEntry<>("b", "value_2"));
  }

  @Test
  public void
      shouldThrowExtensionSettingsMissingExceptionWhenExtensionDoesNotHaveSettingsDefined() {
    // given
    ClassLoader classLoader = getClass().getClassLoader();
    Path path =
        Paths.get(
            Objects.requireNonNull(classLoader.getResource("stargate-config.yaml")).getPath());
    ConfigStoreYaml configStoreYaml = new ConfigStoreYaml(path);

    // when, then
    assertThatThrownBy(() -> configStoreYaml.getConfigForExtension("non_existing_extension"))
        .isInstanceOf(MissingExtensionSettingsException.class)
        .hasMessageContaining(
            "does not contain settings from a given extension: non_existing_extension");
  }

  @Test
  public void shouldThrowYamlConfigExceptionWhenLoadingNonExistingFile() {
    // given
    Path path = Paths.get("non-existing");
    ConfigStoreYaml configStoreYaml = new ConfigStoreYaml(path);

    // when, then
    assertThatThrownBy(() -> configStoreYaml.getConfigForExtension("non_existing_extension"))
        .isInstanceOf(YamlConfigException.class)
        .hasMessageContaining("Problem when processing yaml file from: non-existing");
  }
}
