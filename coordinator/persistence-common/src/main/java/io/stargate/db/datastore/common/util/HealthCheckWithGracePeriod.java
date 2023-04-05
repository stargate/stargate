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

import io.stargate.core.util.TimeSource;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

public abstract class HealthCheckWithGracePeriod {
  protected final Duration gracePeriod;
  private final AtomicLong failureTimestamp = new AtomicLong(-1);
  private final TimeSource timeSource;

  protected HealthCheckWithGracePeriod(Duration gracePeriod, TimeSource timeSource) {
    this.gracePeriod = gracePeriod;
    this.timeSource = timeSource;
  }

  /**
   * Executes the specific logic for determining whether the system is in a healthy state. This
   * method need not deal with the grace period, which is handled by {@link #check()}.
   *
   * @return <code>true</code> if the system is healthy, <code>false</code> otherwise.
   */
  protected abstract boolean isHealthy();

  /**
   * Resets the grace period marker. A new grace period will begin at the next failed health check.
   */
  protected void reset() {
    failureTimestamp.set(-1);
  }

  /**
   * Executes the logic for determining whether the system is in a healthy state and returns the
   * result.
   *
   * <p>Actual health checks are performed by {@link #isHealthy()}, this method additionally applies
   * the grace period (as set in the constructor) to failed checks.
   *
   * @return <code>true</code> if the system is healthy, <code>false</code> otherwise.
   */
  public boolean check() {
    long timestamp = timeSource.currentTimeMillis();

    boolean healthy = isHealthy();

    if (healthy) {
      reset();
      return true;
    }

    if (failureTimestamp.compareAndSet(-1, timestamp)) {
      // first failure after a healthy period - start grace period
      return true;
    }

    long referenceTimestamp = failureTimestamp.get();
    if (referenceTimestamp < 0) {
      // there was a concurrent healthy check - ignore this failure
      return true;
    }

    long graceMillis = gracePeriod.toMillis();
    long deadline = referenceTimestamp + graceMillis;

    return timestamp < deadline;
  }
}
