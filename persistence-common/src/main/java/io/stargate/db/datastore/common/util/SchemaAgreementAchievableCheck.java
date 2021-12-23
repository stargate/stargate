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
package io.stargate.db.datastore.common.util;

import io.stargate.db.util.TimeSource;
import java.time.Duration;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaAgreementAchievableCheck extends HealthCheckWithGracePeriod {

  private static final Logger logger =
      LoggerFactory.getLogger(SchemaAgreementAchievableCheck.class);

  private final Supplier<Boolean> isInSchemaAgreement;
  private final Supplier<Boolean> isStorageInSchemaAgreement;

  public SchemaAgreementAchievableCheck(
      Supplier<Boolean> isInSchemaAgreement,
      Supplier<Boolean> isStorageInSchemaAgreement,
      Duration gracePeriod,
      TimeSource timeSource) {
    super(gracePeriod, timeSource);
    this.isInSchemaAgreement = isInSchemaAgreement;
    this.isStorageInSchemaAgreement = isStorageInSchemaAgreement;
  }

  @Override
  protected boolean isHealthy() {
    if (isInSchemaAgreement.get()) {
      return true;
    }

    if (!isStorageInSchemaAgreement.get()) {
      // Assume storage nodes are still disseminating schema data and will agree eventually.
      // Note: isSchemaAgreementAchievable() is used to determine if the Stargate node should
      // be restarted. In case storage nodes do not agree among themselves, restarting Stargate
      // will not help, so there's no point returning false in this situation even if the
      // schema disagreement at storage level is perpetual.
      logger.debug("Storage nodes are not in schema agreement");
      return true;
    }

    // Storage nodes agree on the schema version, but Stargate has a different schema.
    // Fail the check and allow HealthCheckWithGracePeriod to apply the grace period during
    // which schema pull requests are expected to complete and synchronize the local schema
    // with storage nodes.
    return false;
  }

  @Override
  public boolean check() {
    boolean result = super.check();
    if (!result) {
      logger.error("Schema agreement is not achievable with grace period {}", gracePeriod);
    }

    return result;
  }
}
