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
package io.stargate.db.cdc.config;

import static io.stargate.db.cdc.config.CDCConfigLoader.CDC_CORE_CONFIG_MODULE_NAME;
import static io.stargate.db.cdc.config.CDCConfigLoader.ENABLED_TABLES_SETTINGS_NAME;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import io.stargate.config.store.api.ConfigStore;
import io.stargate.config.store.api.ConfigWithOverrides;
import io.stargate.config.store.yaml.ConfigStoreYaml;
import io.stargate.db.cdc.config.CDCConfigLoader.KeyspaceTable;
import io.stargate.db.schema.Table;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CDCConfigLoaderTest {

  @ParameterizedTest
  @MethodSource("keyspaceTableProvider")
  public void shouldMapToKeyspaceTable(
      String keyspaceAndTable, String expectedKeyspace, String expectedTable) {
    // when
    KeyspaceTable keyspaceTable = CDCConfigLoader.mapToKeyspaceTable(keyspaceAndTable);

    // then
    assertThat(keyspaceTable.getKeyspace()).isEqualTo(expectedKeyspace);
    assertThat(keyspaceTable.getTable()).isEqualTo(expectedTable);
  }

  public static Stream<Arguments> keyspaceTableProvider() {
    return Stream.of(
        arguments("ks1.table1", "ks1", "table1"),
        arguments("\"ks1\".table1", "\"ks1\"", "table1"),
        arguments("XYZ.345", "XYZ", "345"));
  }

  @ParameterizedTest
  @MethodSource("wrongKeyspaceTableProvider")
  public void shouldThrowIfMapToKeyspaceTableFailed(String keyspaceAndTable) {
    // when, then
    assertThatThrownBy(() -> CDCConfigLoader.mapToKeyspaceTable(keyspaceAndTable))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(keyspaceAndTable);
  }

  public static Stream<Arguments> wrongKeyspaceTableProvider() {
    return Stream.of(
        arguments("ks1"),
        arguments("ks1."),
        arguments("ks.1.table"),
        arguments(".table"),
        arguments("."));
  }

  @Test
  public void shouldValidateIfTableIsTackedByCDC() {
    // given
    ConfigStore configStore = mock(ConfigStore.class);
    Map<String, Object> settings = new HashMap<>();
    settings.put(ENABLED_TABLES_SETTINGS_NAME, Collections.singletonList("ks.table"));
    when(configStore.getConfigForModule(CDC_CORE_CONFIG_MODULE_NAME))
        .thenReturn(new ConfigWithOverrides(settings, CDC_CORE_CONFIG_MODULE_NAME));

    // when
    CDCConfig cdcConfig = CDCConfigLoader.loadConfig(configStore);

    // then
    assertThat(cdcConfig.isTrackedByCDC(mockTableWithKeyspace("ks", "table"))).isTrue();
    assertThat(cdcConfig.isTrackedByCDC(mockTableWithKeyspace("ks", "table_not_tracked")))
        .isFalse();
    assertThat(cdcConfig.isTrackedByCDC(mockTableWithKeyspace("ks_not_tracked", "table")))
        .isFalse();
  }

  @Test
  public void shouldSupportRuntimeReloadOfEnabledTablesSetting() {
    // given
    ConfigStore configStore = mock(ConfigStore.class);
    Map<String, Object> settings = new HashMap<>();
    settings.put(ENABLED_TABLES_SETTINGS_NAME, Collections.singletonList("ks.table"));
    when(configStore.getConfigForModule(CDC_CORE_CONFIG_MODULE_NAME))
        .thenReturn(new ConfigWithOverrides(settings, CDC_CORE_CONFIG_MODULE_NAME));

    // when
    CDCConfig cdcConfig = CDCConfigLoader.loadConfig(configStore);

    // then
    assertThat(cdcConfig.isTrackedByCDC(mockTableWithKeyspace("ks", "table"))).isTrue();
    assertThat(cdcConfig.isTrackedByCDC(mockTableWithKeyspace("ks", "table_not_tracked_yet")))
        .isFalse();

    // when reload of underlying settings
    settings.put(
        ENABLED_TABLES_SETTINGS_NAME, Collections.singletonList("ks.table_not_tracked_yet"));

    // then
    assertThat(cdcConfig.isTrackedByCDC(mockTableWithKeyspace("ks", "table"))).isFalse();
    assertThat(cdcConfig.isTrackedByCDC(mockTableWithKeyspace("ks", "table_not_tracked_yet")))
        .isTrue();
  }

  private Table mockTableWithKeyspace(String ks, String table) {
    Table tableMock = mock(Table.class);
    when(tableMock.name()).thenReturn(table);
    when(tableMock.keyspace()).thenReturn(ks);
    return tableMock;
  }

  @Test
  public void shouldValidateIfTableIsTackedByCDCUsingConfigStoreYaml() {
    // given
    Path path =
        Paths.get(
            Objects.requireNonNull(getClass().getClassLoader().getResource("stargate-config.yaml"))
                .getPath());
    ConfigStoreYaml configStoreYaml = new ConfigStoreYaml(path, new MetricRegistry());

    // when
    CDCConfig cdcConfig = CDCConfigLoader.loadConfig(configStoreYaml);

    // then
    assertThat(cdcConfig.isTrackedByCDC(mockTableWithKeyspace("ks1", "table_a"))).isTrue();
    assertThat(cdcConfig.isTrackedByCDC(mockTableWithKeyspace("ks2", "table_b"))).isTrue();
    assertThat(cdcConfig.isTrackedByCDC(mockTableWithKeyspace("ks1", "table_not_tracked")))
        .isFalse();
  }
}
