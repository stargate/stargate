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
package io.stargate.grpc.retries;

import static io.stargate.grpc.retries.RetryDecision.RETHROW;
import static io.stargate.grpc.retries.RetryDecision.RETRY;
import static org.apache.cassandra.stargate.db.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.stargate.db.WriteType.BATCH_LOG;
import static org.apache.cassandra.stargate.db.WriteType.SIMPLE;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.apache.cassandra.stargate.db.WriteType;
import org.apache.cassandra.stargate.exceptions.ReadTimeoutException;
import org.apache.cassandra.stargate.exceptions.WriteTimeoutException;
import org.assertj.core.api.Assert;
import org.junit.jupiter.api.Test;

class DefaultRetryPolicyTest {

  private static final RetryPolicy retryPolicy = new DefaultRetryPolicy();

  @Test
  public void shouldProcessReadTimeouts() {
    assertOnReadTimeout(QUORUM, 2, 2, false, 0).isEqualTo(RETRY);
    assertOnReadTimeout(QUORUM, 2, 2, false, 1).isEqualTo(RETHROW);
    assertOnReadTimeout(QUORUM, 2, 2, true, 0).isEqualTo(RETHROW);
    assertOnReadTimeout(QUORUM, 2, 1, true, 0).isEqualTo(RETHROW);
    assertOnReadTimeout(QUORUM, 2, 1, false, 0).isEqualTo(RETHROW);
  }

  @Test
  public void shouldProcessWriteTimeouts() {
    assertOnWriteTimeout(QUORUM, BATCH_LOG, 2, 0, 0).isEqualTo(RETRY);
    assertOnWriteTimeout(QUORUM, BATCH_LOG, 2, 0, 1).isEqualTo(RETHROW);
    assertOnWriteTimeout(QUORUM, SIMPLE, 2, 0, 0).isEqualTo(RETHROW);
  }

  protected Assert<?, RetryDecision> assertOnReadTimeout(
      ConsistencyLevel cl, int blockFor, int received, boolean dataPresent, int retryCount) {
    return assertThat(
        retryPolicy.onReadTimeout(
            new ReadTimeoutException(cl, received, blockFor, dataPresent), retryCount));
  }

  protected Assert<?, RetryDecision> assertOnWriteTimeout(
      ConsistencyLevel cl, WriteType writeType, int blockFor, int received, int retryCount) {
    return assertThat(
        retryPolicy.onWriteTimeout(
            new WriteTimeoutException(writeType, cl, blockFor, received), retryCount));
  }
}
