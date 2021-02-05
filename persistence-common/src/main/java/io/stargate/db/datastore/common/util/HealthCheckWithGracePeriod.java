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

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.cassandra.utils.TimeSource;

public abstract class HealthCheckWithGracePeriod {
  protected final Duration gracePeriod;
  private final AtomicLong failureTimestamp = new AtomicLong(-1);
  private final TimeSource timeSource;

  protected HealthCheckWithGracePeriod(Duration gracePeriod, TimeSource timeSource) {
    this.gracePeriod = gracePeriod;
    this.timeSource = timeSource;
  }

  protected abstract boolean isHealthy();

  protected void reset() {
    failureTimestamp.set(-1);
  }

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
