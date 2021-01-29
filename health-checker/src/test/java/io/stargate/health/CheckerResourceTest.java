package io.stargate.health;

import static javax.ws.rs.core.Response.Status.OK;
import static javax.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;
import static org.assertj.core.api.Assertions.assertThat;

import com.codahale.metrics.health.HealthCheck;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.Collections;
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
  private BundleService bundleService;
  private CheckerResource checker;

  @BeforeEach
  public void setup() {
    bundleService = Mockito.mock(BundleService.class);
    checker = new CheckerResource(bundleService, healthCheckRegistry);
  }

  @Test
  public void liveness() {
    Mockito.doReturn(false).when(bundleService).checkBundleStates();
    assertThat(checker.checkLiveness().getStatus()).isEqualTo(SERVICE_UNAVAILABLE.getStatusCode());

    Mockito.doReturn(true).when(bundleService).checkBundleStates();
    assertThat(checker.checkLiveness().getStatus()).isEqualTo(OK.getStatusCode());
  }

  @Test
  public void readinessNotStarted() {
    Mockito.doReturn(false).when(bundleService).checkBundleStates();
    assertThat(checker.checkReadiness(null).getStatus())
        .isEqualTo(SERVICE_UNAVAILABLE.getStatusCode());
    assertThat(checker.checkReadiness(Collections.emptySet()).getStatus())
        .isEqualTo(SERVICE_UNAVAILABLE.getStatusCode());
    assertThat(checker.checkReadiness(Collections.singleton("testUnknown")).getStatus())
        .isEqualTo(SERVICE_UNAVAILABLE.getStatusCode());
  }

  @Test
  public void readinessDataStoreUnavailable() {
    Mockito.doReturn(true).when(bundleService).checkBundleStates();
    Mockito.doReturn(false).when(bundleService).checkDataStoreAvailable();
    assertThat(checker.checkReadiness(null).getStatus())
        .isEqualTo(SERVICE_UNAVAILABLE.getStatusCode());
    assertThat(checker.checkReadiness(Collections.emptySet()).getStatus())
        .isEqualTo(SERVICE_UNAVAILABLE.getStatusCode());
    assertThat(checker.checkReadiness(Collections.singleton("testUnknown")).getStatus())
        .isEqualTo(SERVICE_UNAVAILABLE.getStatusCode());
  }

  @Test
  public void readinessEmpty() {
    Mockito.doReturn(true).when(bundleService).checkBundleStates();
    Mockito.doReturn(true).when(bundleService).checkDataStoreAvailable();
    assertThat(checker.checkReadiness(null).getStatus()).isEqualTo(OK.getStatusCode());
    assertThat(checker.checkReadiness(Collections.emptySet()).getStatus())
        .isEqualTo(OK.getStatusCode());
  }

  @Test
  public void readinessMissingCheckExplicit() {
    Mockito.doReturn(true).when(bundleService).checkBundleStates();
    Mockito.doReturn(true).when(bundleService).checkDataStoreAvailable();
    assertThat(checker.checkReadiness(null).getStatus()).isEqualTo(OK.getStatusCode());
    assertThat(checker.checkReadiness(Collections.emptySet()).getStatus())
        .isEqualTo(OK.getStatusCode());
    assertThat(checker.checkReadiness(Collections.singleton("testUnknown")).getStatus())
        .isEqualTo(SERVICE_UNAVAILABLE.getStatusCode());
  }

  @Test
  public void readinessMissingCheckImplicit() {
    Mockito.doReturn(true).when(bundleService).checkBundleStates();
    Mockito.doReturn(true).when(bundleService).checkDataStoreAvailable();
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
    Mockito.doReturn(true).when(bundleService).checkBundleStates();
    Mockito.doReturn(true).when(bundleService).checkDataStoreAvailable();

    healthCheckRegistry.register("test-unhealthy", UNHEALTHY_CHECK);
    healthCheckRegistry.register("test-ok", OK_CHECK);

    assertThat(checker.checkReadiness(null).getStatus())
        .isEqualTo(SERVICE_UNAVAILABLE.getStatusCode());
    assertThat(checker.checkReadiness(Collections.emptySet()).getStatus())
        .isEqualTo(SERVICE_UNAVAILABLE.getStatusCode());
    assertThat(checker.checkReadiness(Collections.singleton("test-unhealthy")).getStatus())
        .isEqualTo(SERVICE_UNAVAILABLE.getStatusCode());
    assertThat(checker.checkReadiness(ImmutableSet.of("test-ok", "test-unhealthy")).getStatus())
        .isEqualTo(SERVICE_UNAVAILABLE.getStatusCode());
    assertThat(checker.checkReadiness(Collections.singleton("test-ok")).getStatus())
        .isEqualTo(OK.getStatusCode());
  }

  @ParameterizedTest
  @CsvSource({"test-unhealthy", "test-ok", "test-ok:test-unhealthy"})
  public void readinessRequiredRegisteredChecks(String defaultChecksString) {
    Set<String> defaultChecks =
        Arrays.stream(defaultChecksString.split(":")).collect(Collectors.toSet());

    Mockito.doReturn(true).when(bundleService).checkBundleStates();
    Mockito.doReturn(true).when(bundleService).checkDataStoreAvailable();

    healthCheckRegistry.register("test-unhealthy", UNHEALTHY_CHECK);
    healthCheckRegistry.register("test-ok", OK_CHECK);

    Mockito.when(bundleService.defaultHealthCheckNames()).thenReturn(defaultChecks);

    assertThat(checker.checkReadiness(null).getStatus())
        .isEqualTo(SERVICE_UNAVAILABLE.getStatusCode());
    assertThat(checker.checkReadiness(Collections.emptySet()).getStatus())
        .isEqualTo(SERVICE_UNAVAILABLE.getStatusCode());
    assertThat(checker.checkReadiness(Collections.singleton("test-unhealthy")).getStatus())
        .isEqualTo(SERVICE_UNAVAILABLE.getStatusCode());
    assertThat(checker.checkReadiness(ImmutableSet.of("test-ok", "test-unhealthy")).getStatus())
        .isEqualTo(SERVICE_UNAVAILABLE.getStatusCode());
    assertThat(checker.checkReadiness(Collections.singleton("test-ok")).getStatus())
        .isEqualTo(OK.getStatusCode());
  }
}
