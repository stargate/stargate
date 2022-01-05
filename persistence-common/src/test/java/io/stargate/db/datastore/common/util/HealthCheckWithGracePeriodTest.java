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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class HealthCheckWithGracePeriodTest {

  private static final Duration TEST_GRACE_PERIOD = Duration.ofSeconds(5);
  private HealthCheckWithGracePeriod check;
  private TimeSource timeSource;
  private boolean healthy;

  @BeforeEach
  public void setup() {
    timeSource = Mockito.mock(TimeSource.class);
    check =
        new HealthCheckWithGracePeriod(TEST_GRACE_PERIOD, timeSource) {
          @Override
          protected boolean isHealthy() {
            return healthy;
          }
        };
  }

  private AbstractBooleanAssert<?> assertThatCheck(long time, boolean healthy) {
    this.healthy = healthy;
    Mockito.doReturn(time).when(timeSource).currentTimeMillis();
    return assertThat(check.check());
  }

  @Test
  public void testSuccess() {
    assertThatCheck(0, true).isTrue();
    assertThatCheck(0, true).isTrue();

    assertThatCheck(TEST_GRACE_PERIOD.toMillis() + 100, true).isTrue();
    assertThatCheck(TEST_GRACE_PERIOD.toMillis() + 100, true).isTrue();
  }

  @Test
  public void testFailWithinGracePeriod() {
    assertThatCheck(0, false).isTrue();
    assertThatCheck(TEST_GRACE_PERIOD.toMillis() - 1, false).isTrue();
  }

  @Test
  public void testFailAfterGracePeriod() {
    assertThatCheck(0, false).isTrue();
    assertThatCheck(TEST_GRACE_PERIOD.toMillis() + 1, false).isFalse();
    assertThatCheck(TEST_GRACE_PERIOD.toMillis() + 1, false).isFalse();
  }

  @Test
  public void testRecoveryWithinGracePeriod() {
    assertThatCheck(0, true).isTrue();
    assertThatCheck(TEST_GRACE_PERIOD.toMillis() - 1, false).isTrue();
    assertThatCheck(TEST_GRACE_PERIOD.toMillis() + 1, true).isTrue();
    assertThatCheck(TEST_GRACE_PERIOD.toMillis() + 2, false).isTrue();
  }

  @Test
  public void testFailAndRecover() {
    assertThatCheck(0, true).isTrue();
    assertThatCheck(100, false).isTrue();
    assertThatCheck(101, false).isTrue();
    assertThatCheck(100 + TEST_GRACE_PERIOD.toMillis() + 1, false).isFalse();
    assertThatCheck(100 + TEST_GRACE_PERIOD.toMillis() + 2, true).isTrue();
  }

  @Test
  public void testReset() {
    assertThatCheck(0, false).isTrue();
    check.reset();
    assertThatCheck(TEST_GRACE_PERIOD.toMillis() + 1, false).isTrue();
    assertThatCheck(2 * TEST_GRACE_PERIOD.toMillis() + 1, false).isFalse();
    check.reset();
    assertThatCheck(2 * TEST_GRACE_PERIOD.toMillis() + 1, false).isTrue();
    assertThatCheck(3 * TEST_GRACE_PERIOD.toMillis() + 1, false).isFalse();
  }
}
