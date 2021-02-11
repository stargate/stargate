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
import io.stargate.db.Persistence;
import java.util.Collection;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaAgreementChecker extends HealthCheck {
  private static final Logger logger = LoggerFactory.getLogger(SchemaAgreementChecker.class);

  private static final String DB_PERSISTENCE_IDENTIFIER =
      System.getProperty("stargate.persistence_id", "CassandraPersistence");
  private static final String DB_PERSISTENCE_FILTER =
      String.format("(Identifier=%s)", DB_PERSISTENCE_IDENTIFIER);

  private final BundleContext context;

  public SchemaAgreementChecker(BundleContext context) {
    this.context = context;
  }

  @Override
  protected Result check() {
    try {
      Collection<ServiceReference<Persistence>> services =
          context.getServiceReferences(Persistence.class, DB_PERSISTENCE_FILTER);

      // at most one persistence implementation is expected to be selected
      for (ServiceReference<Persistence> service : services) {
        Persistence persistence = context.getService(service);
        try {
          if (persistence.isInSchemaAgreement()) {
            return Result.healthy("All schemas agree");
          }

          if (persistence.isSchemaAgreementAchievable()) {
            return Result.healthy("Waiting for schema agreement");
          }

          return Result.unhealthy("Schema agreement is not achievable");
        } finally {
          context.ungetService(service);
        }
      }

      return Result.healthy("Persistence is not initialized yet");

    } catch (Exception e) {
      logger.warn("Schema agreement check failed with {}", e.getMessage(), e);
      return Result.unhealthy("Unable to check schema agreement: " + e);
    }
  }
}
