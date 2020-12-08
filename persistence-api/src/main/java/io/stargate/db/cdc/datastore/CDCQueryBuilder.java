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
import static io.stargate.db.cdc.serde.QuerySerializer.serializeQuery;

import com.datastax.oss.driver.api.core.uuid.Uuids;
import io.stargate.config.store.api.ConfigStore;
import io.stargate.db.cdc.shardmanager.ShardManager;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.query.BoundDMLQuery;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.builder.Replication;
import io.stargate.db.schema.Column;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CDCQueryBuilder {
  private static final Logger logger = LoggerFactory.getLogger(CDCQueryBuilder.class);
  public static final String DEFAULT_CDC_KEYSPACE = "cdc_core";
  public static final String DEFAULT_CDC_EVENTS_TABLE = "cdc_events";
  private final ConfigStore configStore;
  private final DataStore dataStore;
  private final ShardManager shardManager;

  public CDCQueryBuilder(ConfigStore configStore, DataStore dataStore, ShardManager shardManager) {
    this.configStore = configStore;
    this.dataStore = dataStore;
    this.shardManager = shardManager;
  }

  public BoundQuery toInsert(BoundDMLQuery query) {
    return dataStore
        .queryBuilder()
        .insertInto(getKeyspaceName(), getCdcEventsTableName())
        .value(CDCEventsColumns.SHARD.getName(), shardManager.getShardId())
        .value(CDCEventsColumns.EVENT_ID.getName(), generateTimeUUID())
        .value(CDCEventsColumns.PAYLOAD.getName(), serializeQuery(query))
        .value(
            CDCEventsColumns.VERSION.getName(),
            0) // todo https://github.com/stargate/stargate/issues/500
        .value(CDCEventsColumns.DELIVERED_ON_STREAMS.getName(), Collections.emptySet())
        .build()
        .bind();
  }

  private UUID generateTimeUUID() {
    return Uuids.timeBased();
  }

  public void initCDCKeyspace() {
    String keyspaceName = getKeyspaceName();

    logger.info("Initializing keyspace {} for the CDC.", keyspaceName);

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
  }

  public void initCDCEventsTable() {
    String keyspaceName = getKeyspaceName();
    String tableName = getCdcEventsTableName();

    logger.info("Initializing table {}.{} for the CDC.", keyspaceName, tableName);
    try {
      dataStore
          .queryBuilder()
          .create()
          .table(keyspaceName, tableName)
          .ifNotExists()
          .column(CDCEventsColumns.SHARD.getName(), Column.Type.Int, Column.Kind.PartitionKey)
          //              .column("time_bucket", Column.Type.Bigint)
          // todo in https://github.com/stargate/stargate/issues/461
          .column(CDCEventsColumns.EVENT_ID.getName(), Column.Type.Timeuuid, Column.Kind.Clustering)
          .column(CDCEventsColumns.PAYLOAD.getName(), Column.Type.Blob)
          .column(CDCEventsColumns.VERSION.getName(), Column.Type.Int)
          .column(
              CDCEventsColumns.DELIVERED_ON_STREAMS.getName(), Column.Type.Set.of(Column.Type.Uuid))
          .build()
          .execute(ConsistencyLevel.LOCAL_QUORUM)
          .get();
    } catch (Exception ex) {
      throw new RuntimeException("Failed to initialize CDC table", ex);
    }
  }

  private String getCdcEventsTableName() {
    return Optional.ofNullable(
            configStore.getConfigForModule(CDC_CORE_CONFIG_MODULE_NAME).getWithOverrides("table"))
        .orElse(DEFAULT_CDC_EVENTS_TABLE);
  }

  private String getKeyspaceName() {
    return Optional.ofNullable(
            configStore
                .getConfigForModule(CDC_CORE_CONFIG_MODULE_NAME)
                .getWithOverrides("keyspace"))
        .orElse(DEFAULT_CDC_KEYSPACE);
  }

  private enum CDCEventsColumns {
    SHARD("shard"),
    EVENT_ID("event_id"),
    PAYLOAD("payload"),
    VERSION("version"),
    DELIVERED_ON_STREAMS("delivered_on_stream");

    private String name;

    CDCEventsColumns(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }
}
