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

import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.base.Splitter;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import io.stargate.config.store.api.ConfigStore;
import io.stargate.config.store.api.ConfigWithOverrides;
import io.stargate.db.schema.Table;
import java.util.List;

public class CDCConfigLoader {
  private static final Splitter KEYSPACE_TABLE_SPLITTER = Splitter.on('.');

  public static final String CDC_CORE_CONFIG_MODULE_NAME = "cdc.core";
  public static long DEFAULT_PRODUCER_TIMEOUT_MS = 100;
  public static double DEFAULT_ERROR_RATE_THRESHOLD = 0.5;
  public static int DEFAULT_MIN_ERRORS_PER_SECOND = 10;
  public static int DEFAULT_EWMA_INTERVAL_MINUTES = 1;
  public static final String ENABLED_TABLES_SETTINGS_NAME = "enabledTables";

  public static CDCConfig loadConfig(ConfigStore configStore) {
    return new CDCConfig() {
      @Override
      public boolean isTrackedByCDC(Table table) {
        // we need to support runtime reload, therefore every call needs to invoke the
        // getConfigForModule() method
        ConfigWithOverrides configForModule =
            configStore.getConfigForModule(CDC_CORE_CONFIG_MODULE_NAME);
        return configForModule.getSettingValueList(ENABLED_TABLES_SETTINGS_NAME, String.class)
            .stream()
            .map(CDCConfigLoader::mapToKeyspaceTable)
            .anyMatch(v -> isKeyspaceTableTracked(v, table));
      }

      @Override
      public long getProducerTimeoutMs() {
        return DEFAULT_PRODUCER_TIMEOUT_MS;
      }

      @Override
      public double getErrorRateThreshold() {
        return DEFAULT_ERROR_RATE_THRESHOLD;
      }

      @Override
      public int getMinErrorsPerSecond() {
        return DEFAULT_MIN_ERRORS_PER_SECOND;
      }

      @Override
      public int getEWMAIntervalMinutes() {
        return DEFAULT_EWMA_INTERVAL_MINUTES;
      }
    };
  }

  private static boolean isKeyspaceTableTracked(
      KeyspaceTable cdcEnabledKeyspaceTable, Table table) {
    return cdcEnabledKeyspaceTable.table.equals(table.name())
        && cdcEnabledKeyspaceTable.keyspace.equals(table.keyspace());
  }

  @VisibleForTesting
  static KeyspaceTable mapToKeyspaceTable(String keyspaceTable) {
    List<String> keyspaceAndTable =
        Lists.newArrayList(KEYSPACE_TABLE_SPLITTER.split(keyspaceTable));
    if (keyspaceAndTable.size() != 2
        || keyspaceAndTable.get(0).isEmpty()
        || keyspaceAndTable.get(1).isEmpty()) {
      throw new IllegalArgumentException(
          String.format("The keyspaceTable setting: %s is in a wrong format.", keyspaceTable));
    }
    return new KeyspaceTable(keyspaceAndTable.get(0), keyspaceAndTable.get(1));
  }

  static class KeyspaceTable {
    private final String keyspace;
    private final String table;

    KeyspaceTable(String keyspace, String table) {
      this.keyspace = keyspace;
      this.table = table;
    }

    public String getKeyspace() {
      return keyspace;
    }

    public String getTable() {
      return table;
    }
  }
}
