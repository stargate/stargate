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

import edu.umd.cs.findbugs.annotations.NonNull;
import org.apache.cassandra.stargate.exceptions.PreparedQueryNotFoundException;
import org.apache.cassandra.stargate.exceptions.ReadTimeoutException;
import org.apache.cassandra.stargate.exceptions.WriteTimeoutException;

/** Defines the behavior to adopt when a request fails. */
public interface RetryPolicy {

  /**
   * Whether to retry when the server replied with a {@code READ_TIMEOUT} error; this indicates a
   * <b>server-side</b> timeout during a read query, i.e. some replicas did not reply to the
   * coordinator in time.
   *
   * @param readTimeoutException the exception used to determine if the query should be retried.
   * @param retryCount how many times the retry policy has been invoked already for this request
   *     (not counting the current invocation).
   */
  RetryDecision onReadTimeout(@NonNull ReadTimeoutException readTimeoutException, int retryCount);

  /**
   * Whether to retry when the server replied with a {@code WRITE_TIMEOUT} error; this indicates a
   * <b>server-side</b> timeout during a write query, i.e. some replicas did not reply to the
   * coordinator in time.
   *
   * <p>Note that this method will only be invoked for {@link
   * io.stargate.db.Result.Prepared#isIdempotent()} idempotent} requests: when a write times out, it
   * is impossible to determine with 100% certainty whether the mutation was applied or not, so the
   * write is never safe to retry; the driver will rethrow the error directly, without invoking the
   * retry policy.
   *
   * @param writeTimeoutException the exception used to determine if the query should be retried.
   * @param retryCount how many times the retry policy has been invoked already for this request
   *     (not counting the current invocation).
   */
  RetryDecision onWriteTimeout(
      @NonNull WriteTimeoutException writeTimeoutException, int retryCount);

  /**
   * Whether to retry when the server replied with a {@code UNPREPARED} error; this indicates a
   * <b>server-side</b> prepared query cache evicted a prepared statement during a query.
   *
   * @param preparedQueryNotFoundException the exception used to determine if the query should be
   *     retried.
   * @param retryCount how many times the retry policy has been invoked already for this request
   */
  RetryDecision onUnprepared(
      PreparedQueryNotFoundException preparedQueryNotFoundException, int retryCount);
}
