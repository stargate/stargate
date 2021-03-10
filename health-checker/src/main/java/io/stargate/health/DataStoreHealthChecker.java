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
import io.stargate.db.query.builder.Replication;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ImmutableColumn;
import io.stargate.db.schema.ImmutableTable;
import io.stargate.db.schema.Table;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataStoreHealthChecker extends HealthCheck {
  private static final Logger logger = LoggerFactory.getLogger(StargateNodesHealthChecker.class);
  private static final String KEYSPACE_NAME = "data_store_health_check";
  private static final String TABLE_NAME = "dummy_table";
  public static final String PK_COLUMN_VALUE = "dummy_pk";
  public static final String VALUE_COLUMN_VALUE = "dummy_value";
  private final BundleContext context;

  static final String PK_COLUMN_NAME = "pk";
  static final String VALUE_COLUMN_NAME = "value";
  static final Table EXPECTED_TABLE =
      ImmutableTable.builder()
          .keyspace(KEYSPACE_NAME)
          .name(TABLE_NAME)
          .addColumns(
              ImmutableColumn.create(PK_COLUMN_NAME, Column.Kind.PartitionKey, Column.Type.Varchar),
              ImmutableColumn.create(VALUE_COLUMN_NAME, Column.Kind.Regular, Column.Type.Varchar))
          .build();

  public DataStoreHealthChecker(BundleContext context) {
    this.context = context;
  }

  @Override
  protected Result check() throws Exception {
    try {
      ServiceReference<DataStoreFactory> dataStoreFactoryReference =
          context.getServiceReference(DataStoreFactory.class);
      if (dataStoreFactoryReference != null) {
        DataStoreFactory dataStoreFactory = context.getService(dataStoreFactoryReference);
        try {
          DataStore dataStore = dataStoreFactory.createInternal();
          ensureTableExists(dataStore);

          // insert record
          dataStore
              .execute(
                  dataStore
                      .queryBuilder()
                      .insertInto(KEYSPACE_NAME, TABLE_NAME)
                      .value(PK_COLUMN_NAME, PK_COLUMN_VALUE)
                      .value(VALUE_COLUMN_NAME, VALUE_COLUMN_VALUE)
                      .build()
                      .bind())
              .get();

          Future<ResultSet> rs =
              dataStore
                  .queryBuilder()
                  .select()
                  .column(PK_COLUMN_NAME)
                  .column(VALUE_COLUMN_NAME)
                  .from(KEYSPACE_NAME, TABLE_NAME)
                  .build()
                  .execute();

          Row row = rs.get().one();
          String pk = row.getString(PK_COLUMN_NAME);
          String value = row.getString(VALUE_COLUMN_NAME);

          if (pk == null || value == null) {
            return Result.unhealthy("DataStore did not return the proper data.");
          }

          if (pk.equals(PK_COLUMN_VALUE) && value.equals(VALUE_COLUMN_VALUE)) {
            return Result.healthy("DataStore is operational");
          } else {
            return Result.healthy("DataStore did not return the proper data.");
          }
        } finally {
          context.ungetService(dataStoreFactoryReference);
        }
      }

      logger.warn("DataStoreFactory service is not available");
      return Result.unhealthy("DataStoreFactory service is not available");

    } catch (Exception e) {
      logger.warn("checkIsReady failed with {}", e.getMessage(), e);
      return Result.unhealthy("Unable to access DataStore: " + e);
    }
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
                .withReplication(Replication.simpleStrategy(1))
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
