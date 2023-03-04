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
package io.stargate.health;

import com.codahale.metrics.health.HealthCheck;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.db.query.Predicate;
import io.stargate.db.query.builder.Replication;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ImmutableColumn;
import io.stargate.db.schema.ImmutableTable;
import io.stargate.db.schema.Table;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageHealthChecker extends HealthCheck {
  private static final UUID STARGATE_NODE_ID = UUID.randomUUID();
  private static final Logger logger = LoggerFactory.getLogger(DataStoreHealthChecker.class);
  private static final boolean SHOULD_CREATE_KS_AND_TABLE =
      Boolean.parseBoolean(
          System.getProperty("stargate.health_check.data_store.create_ks_and_table", "false"));
  private static final String KEYSPACE_NAME =
      System.getProperty(
          "stargate.health_check.data_store.keyspace_name", "data_store_health_check");
  private static final String TABLE_NAME =
      System.getProperty("stargate.health_check.data_store.table_name", "health_table");
  private static final int INSERT_TTL_SECONDS = 600;
  private static final boolean STORAGE_CHECK_ENABLED =
      Boolean.parseBoolean(System.getProperty("stargate.health_check.data_store.enabled", "false"));
  private static final int REPLICATION_FACTOR =
      Integer.parseInt(
          System.getProperty("stargate.health_check.data_store.replication_factor", "1"));

  private static final String PK_COLUMN_NAME = "pk";
  private static final String VALUE_COLUMN_NAME = "value";
  private static final String VALUE_COLUMN_VALUE = "dummy_value";
  private static final Table EXPECTED_TABLE =
      ImmutableTable.builder()
          .keyspace(KEYSPACE_NAME)
          .name(TABLE_NAME)
          .addColumns(
              ImmutableColumn.create(PK_COLUMN_NAME, Column.Kind.PartitionKey, Column.Type.Uuid),
              ImmutableColumn.create(VALUE_COLUMN_NAME, Column.Kind.Regular, Column.Type.Text))
          .build();
  private final DataStoreFactory dataStoreFactory;

  public StorageHealthChecker(DataStoreFactory dataStoreFactory)
      throws ExecutionException, InterruptedException {

    this.dataStoreFactory = dataStoreFactory;
    if (SHOULD_CREATE_KS_AND_TABLE) {
      ensureTableExists(dataStoreFactory.createInternal());
    }
  }

  @Override
  protected Result check() throws Exception {
    if (!STORAGE_CHECK_ENABLED) {
      return Result.healthy("Storage check disabled");
    }
    try {
      DataStore dataStore = dataStoreFactory.createInternal();

      Instant writeTimestamp = Instant.now();
      // insert record
      dataStore
          .execute(
              dataStore
                  .queryBuilder()
                  .insertInto(KEYSPACE_NAME, TABLE_NAME)
                  .value(PK_COLUMN_NAME, STARGATE_NODE_ID)
                  .value(VALUE_COLUMN_NAME, VALUE_COLUMN_VALUE)
                  .ttl(INSERT_TTL_SECONDS)
                  .timestamp(writeTimestamp.toEpochMilli())
                  .build()
                  .bind(),
              ConsistencyLevel.LOCAL_QUORUM)
          .get();

      // select record
      ResultSet rs =
          dataStore
              .execute(
                  dataStore
                      .queryBuilder()
                      .select()
                      .writeTimeColumn(VALUE_COLUMN_NAME)
                      .from(KEYSPACE_NAME, TABLE_NAME)
                      .where(PK_COLUMN_NAME, Predicate.EQ, STARGATE_NODE_ID)
                      .build()
                      .bind(),
                  ConsistencyLevel.LOCAL_QUORUM)
              .get();

      Row row = rs.one();
      Instant timestampRead = Instant.ofEpochMilli(row.getLong(0));
      if (isGreaterThanOrEqual(timestampRead, writeTimestamp)) {
        return Result.healthy("Storage is operational");
      } else {
        return Result.unhealthy("Storage did not return the proper data.");
      }
    } catch (Exception e) {
      logger.warn("checkIsReady failed with {}", e.getMessage(), e);
      return Result.unhealthy("Unable to access Storage: " + e);
    }
  }

  private boolean isGreaterThanOrEqual(Instant timestampRead, Instant expectedTimestamp) {
    if (timestampRead == null) {
      return false;
    }
    // in case there was a concurrent update, validate that the timestampRead is greater than
    // expected timestamp
    return expectedTimestamp.equals(timestampRead) || timestampRead.isAfter(expectedTimestamp);
  }

  private void ensureTableExists(DataStore dataStore)
      throws ExecutionException, InterruptedException {
    // create a dedicated health-check keyspace to not risk conflicts with users keyspace
    dataStore
        .execute(
            dataStore
                .queryBuilder()
                .create()
                .keyspace(KEYSPACE_NAME)
                .ifNotExists()
                .withReplication(Replication.simpleStrategy(REPLICATION_FACTOR))
                .build()
                .bind())
        .get();

    // create table
    dataStore
        .execute(
            dataStore
                .queryBuilder()
                .create()
                .table(KEYSPACE_NAME, TABLE_NAME)
                .ifNotExists()
                .column(EXPECTED_TABLE.columns())
                .build()
                .bind())
        .get();
  }
}
