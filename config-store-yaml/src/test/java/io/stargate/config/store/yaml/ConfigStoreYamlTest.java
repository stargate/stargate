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

import static io.stargate.config.store.yaml.metrics.MetricsHelper.getMetricValue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codahale.metrics.MetricRegistry;
import io.stargate.config.store.api.ConfigWithOverrides;
import io.stargate.config.store.api.MissingModuleSettingsException;
import io.stargate.config.store.yaml.metrics.CacheMetricsRegistry;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class ConfigStoreYamlTest {

  @Test
  public void shouldGetSettingFromConfigYaml() {
    // given
    Path path =
        Paths.get(
            Objects.requireNonNull(getClass().getClassLoader().getResource("stargate-config.yaml"))
                .getPath());
    ConfigStoreYaml configStoreYaml = new ConfigStoreYaml(path, new MetricRegistry());

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
    ConfigStoreYaml configStoreYaml = new ConfigStoreYaml(path, new MetricRegistry());

    // when, then
    assertThatThrownBy(() -> configStoreYaml.getConfigForModule("non_existing_module"))
        .isInstanceOf(MissingModuleSettingsException.class)
        .hasMessageContaining("does not contain settings from a given module: non_existing_module");
  }

  @Test
  public void shouldThrowYamlConfigExceptionWhenLoadingNonExistingFile() {
    // given
    Path path = Paths.get("non-existing");
    ConfigStoreYaml configStoreYaml = new ConfigStoreYaml(path, new MetricRegistry());

    // when, then
    assertThatThrownBy(() -> configStoreYaml.getConfigForModule("non_existing_module"))
        .isInstanceOf(CompletionException.class)
        .hasMessageContaining("Problem when trying to load YAML config file")
        .hasMessageContaining("for module 'non_existing_module'")
        .hasMessageContaining("(from: 'non-existing')");
  }

  @Test
  public void shouldEvictLoadedContentAfterDefaultEvictionTimeAndExposeStatsOnMetricRegistry() {
    // given
    FakeTicker ticker = new FakeTicker();
    Path path =
        Paths.get(
            Objects.requireNonNull(getClass().getClassLoader().getResource("stargate-config.yaml"))
                .getPath());
    MetricRegistry metricRegistry = new MetricRegistry();
    ConfigStoreYaml configStoreYaml = new ConfigStoreYaml(path, ticker, metricRegistry);

    // when
    assertThat(configStoreYaml.getConfigForModule("extension-1").getConfigMap()).isNotEmpty();

    // then
    assertThat(configStoreYaml.configFileCache.estimatedSize()).isEqualTo(1L);
    assertThat(configStoreYaml.configFileCache.stats().evictionCount()).isEqualTo(0);

    // when
    ticker.advance(ConfigStoreYaml.DEFAULT_EVICTION_TIME.minusSeconds(1));

    // loading value does not refresh eviction time
    assertThat(configStoreYaml.getConfigForModule("extension-1").getConfigMap()).isNotEmpty();
    ticker.advance(Duration.ofSeconds(1));

    // the eviction is done on the load operation - to trigger this we need to call the
    // getConfigForModule method
    assertThat(configStoreYaml.getConfigForModule("extension-1").getConfigMap()).isNotEmpty();

    // then
    assertThat(configStoreYaml.configFileCache.stats().evictionCount()).isEqualTo(1L);
    assertThat(getMetricValue(metricRegistry, CacheMetricsRegistry.SIZE)).isEqualTo(1L);
    assertThat(getMetricValue(metricRegistry, CacheMetricsRegistry.EVICTION_COUNT)).isEqualTo(1L);
  }
}
