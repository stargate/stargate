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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

import io.stargate.db.Persistence;
import java.util.List;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SchemaAgreementCheckerTest {

  @Mock Persistence persistence;

  SchemaAgreementChecker checker;

  @BeforeEach
  public void setup() {
    checker = new SchemaAgreementChecker();
    checker.persistenceServices = List.of(persistence);
  }

  @Test
  public void shouldSucceedWithNoPersistence() {
    checker = new SchemaAgreementChecker();
    checker.persistenceServices = List.of();

    HealthCheckResponse result = checker.call();

    assertThat(result.getStatus()).isEqualTo(HealthCheckResponse.Status.UP);
  }

  @Test
  public void shouldSucceedWhenSchemasAgree() {
    doReturn(true).when(persistence).isInSchemaAgreement();

    HealthCheckResponse result = checker.call();

    assertThat(result.getStatus()).isEqualTo(HealthCheckResponse.Status.UP);
  }

  @Test
  public void shouldSucceedWhenSchemaAgreesWithStorage() {
    doReturn(false).when(persistence).isInSchemaAgreement();
    doReturn(true).when(persistence).isInSchemaAgreementWithStorage();

    HealthCheckResponse result = checker.call();

    assertThat(result.getStatus()).isEqualTo(HealthCheckResponse.Status.UP);
  }

  @Test
  public void shouldSucceedWhenAgreementIsAchievable() {
    doReturn(false).when(persistence).isInSchemaAgreement();
    doReturn(true).when(persistence).isSchemaAgreementAchievable();

    HealthCheckResponse result = checker.call();

    assertThat(result.getStatus()).isEqualTo(HealthCheckResponse.Status.UP);
  }

  @Test
  public void shouldFailWhenAgreementIsNotAchievable() {
    doReturn(false).when(persistence).isInSchemaAgreement();
    doReturn(false).when(persistence).isSchemaAgreementAchievable();

    HealthCheckResponse result = checker.call();

    assertThat(result.getStatus()).isEqualTo(HealthCheckResponse.Status.DOWN);
  }

  @Test
  public void shouldFailOnException() {
    doThrow(new RuntimeException("test-exception")).when(persistence).isInSchemaAgreement();

    HealthCheckResponse result = checker.call();

    assertThat(result.getStatus()).isEqualTo(HealthCheckResponse.Status.DOWN);
  }
}
