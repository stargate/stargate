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

import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.core.util.TimeSource;
import java.time.Duration;
import org.assertj.core.api.AbstractBooleanAssert;
import org.junit.jupiter.api.Test;

class SchemaAgreementAchievableCheckTest {

  private AbstractBooleanAssert<?> assertHealthy(
      boolean schemasAgree, boolean storageSchemasAgree) {
    SchemaAgreementAchievableCheck check =
        new SchemaAgreementAchievableCheck(
            () -> schemasAgree, () -> storageSchemasAgree, Duration.ofMillis(5), TimeSource.SYSTEM);
    return assertThat(check.isHealthy());
  }

  @Test
  public void shouldBeHealthyWhenSchemasAgree() {
    assertHealthy(true, true).isTrue();
    assertHealthy(true, false).isTrue();
  }

  @Test
  public void shouldBeHealthyWhenStorageSchemasDisagree() {
    assertHealthy(false, false).isTrue();
  }

  @Test
  public void shouldNotBeHealthyWhenSchemaIsDifferentFromStorageSchema() {
    assertHealthy(false, true).isFalse();
  }
}
