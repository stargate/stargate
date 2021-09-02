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
import org.apache.cassandra.stargate.exceptions.ReadTimeoutException;
import org.apache.cassandra.stargate.exceptions.WriteTimeoutException;

/** Defines the behavior to adopt when a request fails. */
public interface RetryPolicy {

  /**
   * Whether to retry when the server replied with a {@code READ_TIMEOUT} error; this indicates a
   * <b>server-side</b> timeout during a read query, i.e. some replicas did not reply to the
   * coordinator in time.
   *
   * <p>{@link ReadTimeoutException#consistency} the requested consistency level. {@link
   * ReadTimeoutException#blockFor} the minimum number of replica acknowledgements/responses that
   * were required to fulfill the operation. {@link ReadTimeoutException#received} the number of
   * replica that had acknowledged/responded to the operation before it failed. {@link
   * ReadTimeoutException#dataPresent} whether the actual data was amongst the received replica
   * responses.
   *
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
   * <p>{@link WriteTimeoutException#consistency} the requested consistency level. {@link
   * WriteTimeoutException#writeType} the type of the write for which the timeout was raised. {@link
   * WriteTimeoutException#blockFor} the minimum number of replica acknowledgements/responses that
   * were required to fulfill the operation. {@link WriteTimeoutException#received} the number of
   * replica that had acknowledged/responded to the operation before it failed.
   *
   * @param retryCount how many times the retry policy has been invoked already for this request
   *     (not counting the current invocation).
   */
  RetryDecision onWriteTimeout(
      @NonNull WriteTimeoutException writeTimeoutException, int retryCount);
}
