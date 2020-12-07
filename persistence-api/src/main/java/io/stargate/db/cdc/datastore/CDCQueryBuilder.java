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
package io.stargate.db.cdc.datastore;

import static io.stargate.db.cdc.config.CDCConfigLoader.CDC_CORE_CONFIG_MODULE_NAME;

import io.stargate.config.store.api.ConfigStore;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.builder.Replication;
import io.stargate.db.schema.Column;
import java.util.Optional;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CDCQueryBuilder {
  private static final Logger logger = LoggerFactory.getLogger(CDCEnabledDataStore.class);
  public static final String DEFAULT_CDC_KEYSPACE = "cdc_core";
  public static final String DEFAULT_CDC_TABLE = "cdc_events";
  private final ConfigStore configStore;

  public CDCQueryBuilder(ConfigStore configStore) {
    this.configStore = configStore;
  }

  public BoundQuery toInsert(BoundQuery query) {
    return query; // todo https://github.com/stargate/stargate/issues/460
  }

  public void initCDCKeyspaceTable(DataStore dataStore) {
    String keyspaceName =
        Optional.ofNullable(
                configStore
                    .getConfigForModule(CDC_CORE_CONFIG_MODULE_NAME)
                    .getWithOverrides("keyspace"))
            .orElse(DEFAULT_CDC_KEYSPACE);

    String tableName =
        Optional.ofNullable(
                configStore
                    .getConfigForModule(CDC_CORE_CONFIG_MODULE_NAME)
                    .getWithOverrides("table"))
            .orElse(DEFAULT_CDC_TABLE);

    logger.info("Initializing keyspace {} and table {} for the CDC.", keyspaceName, tableName);

    try {
      dataStore
          .queryBuilder()
          .create()
          .keyspace(keyspaceName)
          .ifNotExists()
          .withReplication(Replication.simpleStrategy(1)) // todo is this a good default?
          .build()
          .execute(ConsistencyLevel.LOCAL_QUORUM)
          .get();
    } catch (Exception ex) {
      throw new RuntimeException("Failed to initialize CDC keyspace", ex);
    }

    try {
      dataStore
          .queryBuilder()
          .create()
          .table(keyspaceName, tableName)
          .ifNotExists()
          .column("shard", Column.Type.Int, Column.Kind.PartitionKey)
          //              .column("time_bucket", Column.Type.Bigint)
          // todo in https://github.com/stargate/stargate/issues/461
          .column("event_id", Column.Type.Timeuuid, Column.Kind.Clustering)
          .column("payload", Column.Type.Blob)
          .column("version", Column.Type.Int)
          .column("delivered_on_stream", Column.Type.Set.of(Column.Type.Uuid))
          .build()
          .execute(ConsistencyLevel.LOCAL_QUORUM)
          .get();
    } catch (Exception ex) {
      throw new RuntimeException("Failed to initialize CDC table", ex);
    }
  }
}
