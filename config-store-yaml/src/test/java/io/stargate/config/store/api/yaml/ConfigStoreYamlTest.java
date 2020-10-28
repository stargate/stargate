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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.util.concurrent.UncheckedExecutionException;
import io.stargate.config.store.api.ConfigWithOverrides;
import io.stargate.config.store.api.MissingModuleSettingsException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Objects;
import org.junit.jupiter.api.Test;

class ConfigStoreYamlTest {

  @Test
  public void shouldGetSettingFromConfigYaml() {
    // given
    Path path =
        Paths.get(
            Objects.requireNonNull(getClass().getClassLoader().getResource("stargate-config.yaml"))
                .getPath());
    ConfigStoreYaml configStoreYaml = new ConfigStoreYaml(path);

    // when
    ConfigWithOverrides configForModule1 = configStoreYaml.getConfigForModule("extension-1");
    ConfigWithOverrides configForModule2 = configStoreYaml.getConfigForModule("extension-2");

    // then
    assertThat(configForModule1.getConfigMap())
        .containsOnly(new SimpleEntry<>("a", 1), new SimpleEntry<>("b", "value"));
    assertThat(configForModule2.getConfigMap())
        .containsOnly(new SimpleEntry<>("a", 2), new SimpleEntry<>("b", Arrays.asList("a", "b")));
  }

  @Test
  public void shouldThrowModuleSettingsMissingExceptionWhenModuleDoesNotHaveSettingsDefined() {
    // given
    Path path =
        Paths.get(
            Objects.requireNonNull(getClass().getClassLoader().getResource("stargate-config.yaml"))
                .getPath());
    ConfigStoreYaml configStoreYaml = new ConfigStoreYaml(path);

    // when, then
    assertThatThrownBy(() -> configStoreYaml.getConfigForModule("non_existing_module"))
        .isInstanceOf(MissingModuleSettingsException.class)
        .hasMessageContaining("does not contain settings from a given module: non_existing_module");
  }

  @Test
  public void shouldThrowYamlConfigExceptionWhenLoadingNonExistingFile() {
    // given
    Path path = Paths.get("non-existing");
    ConfigStoreYaml configStoreYaml = new ConfigStoreYaml(path);

    // when, then
    assertThatThrownBy(() -> configStoreYaml.getConfigForModule("non_existing_module"))
        .isInstanceOf(UncheckedExecutionException.class)
        .hasMessageContaining("Problem when processing yaml file from: non-existing");
  }

  @Test
  public void shouldEvictLoadedContentAfterDefaultEvictionTime() {
    // given
    FakeTicker ticker = new FakeTicker();
    Path path =
        Paths.get(
            Objects.requireNonNull(getClass().getClassLoader().getResource("stargate-config.yaml"))
                .getPath());
    ConfigStoreYaml configStoreYaml = new ConfigStoreYaml(path, ticker);

    // when
    assertThat(configStoreYaml.getConfigForModule("extension-1").getConfigMap()).isNotEmpty();

    // then
    assertThat(configStoreYaml.configFileCache.size()).isEqualTo(1);
    assertThat(configStoreYaml.configFileCache.stats().evictionCount()).isEqualTo(0);

    // when
    ticker.advance(ConfigStoreYaml.DEFAULT_EVICTION_TIME.plus(Duration.ofSeconds(30)));
    // the eviction is done on the load operation
    assertThat(configStoreYaml.getConfigForModule("extension-1").getConfigMap()).isNotEmpty();

    // then
    assertThat(configStoreYaml.configFileCache.size()).isEqualTo(1);
    assertThat(configStoreYaml.configFileCache.stats().evictionCount()).isEqualTo(1);
  }
}
