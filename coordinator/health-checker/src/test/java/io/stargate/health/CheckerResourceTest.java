package io.stargate.health;

import static io.stargate.health.HealthCheckerActivator.BUNDLES_CHECK_NAME;
import static io.stargate.health.HealthCheckerActivator.SCHEMA_CHECK_NAME;
import static javax.ws.rs.core.Response.Status.OK;
import static javax.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;
import static org.assertj.core.api.Assertions.assertThat;

import com.codahale.metrics.health.HealthCheck;
import com.codahale.metrics.health.HealthCheck.Result;
import com.codahale.metrics.health.HealthCheckRegistry;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;

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
class CheckerResourceTest {

  private static final HealthCheck UNHEALTHY_CHECK =
      new HealthCheck() {
        @Override
        protected Result check() {
          return Result.unhealthy("test-message");
        }
      };

  private static final HealthCheck OK_CHECK =
      new HealthCheck() {
        @Override
        protected Result check() {
          return Result.healthy("test-message");
        }
      };

  private final HealthCheckRegistry healthCheckRegistry = new HealthCheckRegistry();
  private HealthCheck bundleHealth;
  private HealthCheck schemaHealth;
  private BundleService bundleService;
  private CheckerResource checker;

  @BeforeEach
  public void setup() {
    bundleService = Mockito.mock(BundleService.class);
    bundleHealth = Mockito.mock(HealthCheck.class);
    schemaHealth = Mockito.mock(HealthCheck.class);
    checker = new CheckerResource(bundleService, healthCheckRegistry);
  }

  @Test
  public void liveness() {
    // bundleHealth is ok but not registered -> SERVICE_UNAVAILABLE
    Mockito.when(bundleHealth.execute()).thenReturn(Result.healthy("test-ok"));
    assertThat(checker.checkLiveness().getStatus()).isEqualTo(SERVICE_UNAVAILABLE.getStatusCode());

    healthCheckRegistry.register(BUNDLES_CHECK_NAME, bundleHealth);

    // schemaHealth is ok but not registered -> SERVICE_UNAVAILABLE
    Mockito.when(schemaHealth.execute()).thenReturn(Result.healthy("test-ok"));
    assertThat(checker.checkLiveness().getStatus()).isEqualTo(SERVICE_UNAVAILABLE.getStatusCode());

    healthCheckRegistry.register(SCHEMA_CHECK_NAME, schemaHealth);
    assertThat(checker.checkLiveness().getStatus()).isEqualTo(OK.getStatusCode());

    Mockito.when(bundleHealth.execute()).thenReturn(Result.unhealthy("test-unhealthy"));
    assertThat(checker.checkLiveness().getStatus()).isEqualTo(SERVICE_UNAVAILABLE.getStatusCode());

    Mockito.when(bundleHealth.execute()).thenReturn(Result.healthy("test-ok"));
    Mockito.when(schemaHealth.execute()).thenReturn(Result.unhealthy("test-unhealthy"));
    assertThat(checker.checkLiveness().getStatus()).isEqualTo(SERVICE_UNAVAILABLE.getStatusCode());
  }

  @Test
  public void readinessEmpty() {
    assertThat(checker.checkReadiness(null).getStatus()).isEqualTo(OK.getStatusCode());
    assertThat(checker.checkReadiness(Collections.emptySet()).getStatus())
        .isEqualTo(OK.getStatusCode());
  }

  @Test
  public void readinessMissingCheckExplicit() {
    assertThat(checker.checkReadiness(null).getStatus()).isEqualTo(OK.getStatusCode());
    assertThat(checker.checkReadiness(Collections.emptySet()).getStatus())
        .isEqualTo(OK.getStatusCode());
    assertThat(checker.checkReadiness(Collections.singleton("testUnknown")).getStatus())
        .isEqualTo(SERVICE_UNAVAILABLE.getStatusCode());
  }

  @Test
  public void readinessMissingCheckImplicit() {
    Mockito.when(bundleService.defaultHealthCheckNames())
        .thenReturn(Collections.singleton("testUnknown"));
    assertThat(checker.checkReadiness(null).getStatus())
        .isEqualTo(SERVICE_UNAVAILABLE.getStatusCode());
    assertThat(checker.checkReadiness(Collections.emptySet()).getStatus())
        .isEqualTo(SERVICE_UNAVAILABLE.getStatusCode());
    assertThat(checker.checkReadiness(Collections.singleton("testUnknown")).getStatus())
        .isEqualTo(SERVICE_UNAVAILABLE.getStatusCode());
  }

  @Test
  public void readinessAllRegisteredChecks() {
    healthCheckRegistry.register("test-unhealthy", UNHEALTHY_CHECK);
    healthCheckRegistry.register("test-ok", OK_CHECK);

    assertThat(checker.checkReadiness(null).getStatus())
        .isEqualTo(SERVICE_UNAVAILABLE.getStatusCode());
    assertThat(checker.checkReadiness(Collections.emptySet()).getStatus())
        .isEqualTo(SERVICE_UNAVAILABLE.getStatusCode());
    assertThat(checker.checkReadiness(Collections.singleton("test-unhealthy")).getStatus())
        .isEqualTo(SERVICE_UNAVAILABLE.getStatusCode());
    assertThat(
            checker
                .checkReadiness(new HashSet<>(Arrays.asList("test-ok", "test-unhealthy")))
                .getStatus())
        .isEqualTo(SERVICE_UNAVAILABLE.getStatusCode());
    assertThat(checker.checkReadiness(Collections.singleton("test-ok")).getStatus())
        .isEqualTo(OK.getStatusCode());
  }

  @ParameterizedTest
  @CsvSource({"test-unhealthy", "test-ok", "test-ok:test-unhealthy"})
  public void readinessRequiredRegisteredChecks(String defaultChecksString) {
    Set<String> defaultChecks =
        Arrays.stream(defaultChecksString.split(":")).collect(Collectors.toSet());

    healthCheckRegistry.register("test-unhealthy", UNHEALTHY_CHECK);
    healthCheckRegistry.register("test-ok", OK_CHECK);

    Mockito.when(bundleService.defaultHealthCheckNames()).thenReturn(defaultChecks);

    assertThat(checker.checkReadiness(null).getStatus())
        .isEqualTo(SERVICE_UNAVAILABLE.getStatusCode());
    assertThat(checker.checkReadiness(Collections.emptySet()).getStatus())
        .isEqualTo(SERVICE_UNAVAILABLE.getStatusCode());
    assertThat(checker.checkReadiness(Collections.singleton("test-unhealthy")).getStatus())
        .isEqualTo(SERVICE_UNAVAILABLE.getStatusCode());
    assertThat(
            checker
                .checkReadiness(new HashSet<>(Arrays.asList("test-ok", "test-unhealthy")))
                .getStatus())
        .isEqualTo(SERVICE_UNAVAILABLE.getStatusCode());
    assertThat(checker.checkReadiness(Collections.singleton("test-ok")).getStatus())
        .isEqualTo(OK.getStatusCode());
  }
}
