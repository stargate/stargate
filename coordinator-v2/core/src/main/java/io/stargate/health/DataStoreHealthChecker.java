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

import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.UUID;
import java.util.concurrent.Future;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Readiness;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Readiness
@ApplicationScoped
public class DataStoreHealthChecker implements HealthCheck {
  private static final Logger logger = LoggerFactory.getLogger(DataStoreHealthChecker.class);

  private static final String NAME = "DataStore";

  private final DataStoreFactory dataStoreFactory;

  @Inject
  public DataStoreHealthChecker(DataStoreFactory dataStoreFactory) {
    this.dataStoreFactory = dataStoreFactory;
  }

  @Override
  public HealthCheckResponse call() {
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
        return HealthCheckResponse.named(NAME)
            .down()
            .withData("status", "Empty cluster name: " + clusterName)
            .build();
      }

      if (schemaVersion == null) {
        return HealthCheckResponse.named(NAME)
            .down()
            .withData("status", "Null schema version")
            .build();
      }

      return HealthCheckResponse.named(NAME)
          .up()
          .withData("status", "DataStore is operational")
          .build();
    } catch (Exception e) {
      logger.warn("checkIsReady failed with {}", e.getMessage(), e);

      return HealthCheckResponse.named(NAME)
          .down()
          .withData("status", "Unable to access DataStore: " + e)
          .build();
    }
  }
}
