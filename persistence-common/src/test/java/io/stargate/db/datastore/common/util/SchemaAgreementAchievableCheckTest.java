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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.utils.SystemTimeSource;
import org.assertj.core.api.AbstractBooleanAssert;
import org.junit.jupiter.api.Test;

class SchemaAgreementAchievableCheckTest {

  private AbstractBooleanAssert<?> assertHealthy(
      boolean schemasAgree, boolean storageSchemasAgree) {
    SchemaAgreementAchievableCheck check =
        new SchemaAgreementAchievableCheck(
            () -> schemasAgree,
            () -> storageSchemasAgree,
            Duration.ofMillis(5),
            new SystemTimeSource());
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

  @Test
  public void shouldResetOnSchemaChange() throws UnknownHostException {
    AtomicLong time = new AtomicLong(0);

    SchemaAgreementAchievableCheck check =
        new SchemaAgreementAchievableCheck(
            () -> false,
            () -> true,
            Duration.ofMillis(5),
            new SystemTimeSource() {
              @Override
              public long currentTimeMillis() {
                return time.get();
              }
            });

    assertThat(check.check()).isTrue();

    check.onChange(InetAddress.getLocalHost(), ApplicationState.SCHEMA, null);
    time.set(10);
    assertThat(check.check()).isTrue();

    time.set(20);
    assertThat(check.check()).isFalse();

    check.onChange(InetAddress.getLocalHost(), ApplicationState.SCHEMA, null);
    assertThat(check.check()).isTrue();
  }
}
