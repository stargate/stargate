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
import java.util.UUID;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataStoreHealthChecker extends HealthCheck {
  private static final Logger logger = LoggerFactory.getLogger(DataStoreHealthChecker.class);

  private final DataStoreFactory dataStoreFactory;

  public DataStoreHealthChecker(DataStoreFactory dataStoreFactory) {
    this.dataStoreFactory = dataStoreFactory;
  }

  @Override
  protected Result check() {
    try {
      DataStore dataStore = dataStoreFactory.createInternal();

      Future<ResultSet> rs =
          dataStore
              .queryBuilder()
              .select()
              .column("cluster_name")
              .column("schema_version")
              .from("system", "local")
              .build()
              .execute();

      Row row = rs.get().one();
      String clusterName = row.getString("cluster_name");
      UUID schemaVersion = row.getUuid("schema_version");

      if (clusterName == null || clusterName.isEmpty()) {
        return Result.unhealthy("Empty cluster name: " + clusterName);
      }

      if (schemaVersion == null) {
        return Result.unhealthy("Null schema version");
      }

      return Result.healthy("DataStore is operational");
    } catch (Exception e) {
      logger.warn("checkIsReady failed with {}", e.getMessage(), e);
      return Result.unhealthy("Unable to access DataStore: " + e);
    }
  }
}
