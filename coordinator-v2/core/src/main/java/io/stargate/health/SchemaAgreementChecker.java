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

import io.quarkus.arc.All;
import io.stargate.db.Persistence;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Liveness;
import org.eclipse.microprofile.health.Readiness;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Health check for the storage schema agreement. */
@Liveness
@Readiness
@ApplicationScoped
public class SchemaAgreementChecker implements HealthCheck {

  private static final Logger logger = LoggerFactory.getLogger(SchemaAgreementChecker.class);

  private static final String NAME = "Schema agreement";

  @Inject @All List<Persistence> persistenceServices;

  @Override
  public HealthCheckResponse call() {
    try {
      // at most one persistence implementation is expected to be selected
      for (Persistence persistence : persistenceServices) {
        if (persistence.isInSchemaAgreement()) {
          return HealthCheckResponse.named(NAME)
              .up()
              .withData("status", "All schemas agree")
              .build();
        }

        // The purpose of the health check is to indicate when restarting this node can help
        // it to achieve schema agreement. Since schema is never actively pulled from other
        // Stargate nodes, restarting will not be effective when local schema agrees with
        // storage nodes and disagrees only with some other Stargate nodes. It is expected
        // that only the Stargate node that does not agree with storage will fail its health
        // check and will be restarted as appropriate.
        if (persistence.isInSchemaAgreementWithStorage()) {
          return HealthCheckResponse.named(NAME)
              .up()
              .withData("status", "Local schema is in agreement with storage nodes")
              .build();
        }

        if (persistence.isSchemaAgreementAchievable()) {
          return HealthCheckResponse.named(NAME)
              .up()
              .withData("status", "Waiting for schema agreement")
              .build();
        }
        return HealthCheckResponse.named(NAME)
            .down()
            .withData("status", "Schema agreement is not achievable")
            .build();
      }

      return HealthCheckResponse.named(NAME)
          .up()
          .withData("status", "Persistence is not initialized yet")
          .build();

    } catch (Exception e) {
      logger.warn("Schema agreement check failed with {}", e.getMessage(), e);

      return HealthCheckResponse.named(NAME)
          .down()
          .withData("status", "Unable to check schema agreement: " + e)
          .build();
    }
  }
}
