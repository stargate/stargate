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

import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.NonNull;
import net.jcip.annotations.ThreadSafe;
import org.apache.cassandra.stargate.db.WriteType;
import org.apache.cassandra.stargate.exceptions.PreparedQueryNotFoundException;
import org.apache.cassandra.stargate.exceptions.ReadTimeoutException;
import org.apache.cassandra.stargate.exceptions.WriteTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default retry policy.
 *
 * <p>This is a very conservative implementation: it triggers a maximum of one retry per request,
 * and only in cases that have a high chance of success (see the method javadocs for detailed
 * explanations of each case). The exception is the {@link
 * RetryPolicy#onUnprepared(PreparedQueryNotFoundException, int)}, which allows limitless retries.
 */
@ThreadSafe
public class DefaultRetryPolicy implements RetryPolicy {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultRetryPolicy.class);

  @VisibleForTesting
  public static final String RETRYING_ON_READ_TIMEOUT =
      "Retrying on read timeout (consistency: {}, required responses: {}, "
          + "received responses: {}, data retrieved: {}, retries: {})";

  @VisibleForTesting
  public static final String RETRYING_ON_WRITE_TIMEOUT =
      "Retrying on write timeout (consistency: {}, write type: {}, "
          + "required acknowledgments: {}, received acknowledgments: {}, retries: {})";

  @VisibleForTesting
  public static final String RETRYING_ON_UNPREPARED =
      "Retrying on unprepared (MD5 digest: {}, retries: {})";

  /**
   * {@inheritDoc}
   *
   * <p>This implementation triggers a maximum of one retry (to the same node), and only if enough
   * replicas had responded to the read request but data was not retrieved amongst those. That
   * usually means that enough replicas are alive to satisfy the consistency, but the coordinator
   * picked a dead one for data retrieval, not having detected that replica as dead yet. The
   * reasoning is that by the time we get the timeout, the dead replica will likely have been
   * detected as dead and the retry has a high chance of success.
   *
   * <p>Otherwise, the exception is rethrown.
   */
  @Override
  public RetryDecision onReadTimeout(@NonNull ReadTimeoutException rte, int retryCount) {

    RetryDecision decision =
        (retryCount == 0 && rte.received >= rte.blockFor && !rte.dataPresent)
            ? RetryDecision.RETRY
            : RetryDecision.RETHROW;

    if (decision == RetryDecision.RETRY && LOG.isTraceEnabled()) {
      LOG.trace(
          RETRYING_ON_READ_TIMEOUT, rte.consistency, rte.blockFor, rte.received, false, retryCount);
    }

    return decision;
  }

  /**
   * {@inheritDoc}
   *
   * <p>This implementation triggers a maximum of one retry, and only for a {@code
   * WriteType.BATCH_LOG} write. The reasoning is that the coordinator tries to write the
   * distributed batch log against a small subset of nodes in the local datacenter; a timeout
   * usually means that none of these nodes were alive but the coordinator hadn't detected them as
   * dead yet. By the time we get the timeout, the dead nodes will likely have been detected as
   * dead, and the retry has thus a high chance of success.
   *
   * <p>Otherwise, the exception is rethrown.
   */
  @Override
  public RetryDecision onWriteTimeout(@NonNull WriteTimeoutException wte, int retryCount) {

    RetryDecision decision =
        (retryCount == 0 && wte.writeType == WriteType.BATCH_LOG)
            ? RetryDecision.RETRY
            : RetryDecision.RETHROW;

    if (decision == RetryDecision.RETRY && LOG.isTraceEnabled()) {
      LOG.trace(
          RETRYING_ON_WRITE_TIMEOUT,
          wte.consistency,
          wte.writeType,
          wte.blockFor,
          wte.received,
          retryCount);
    }
    return decision;
  }

  /**
   * {@inheritDoc}
   *
   * <p>No limits on the retries when UNPREPARED occurs.
   */
  @Override
  public RetryDecision onUnprepared(PreparedQueryNotFoundException pe, int retryCount) {
    if (LOG.isTraceEnabled()) {
      LOG.trace(RETRYING_ON_UNPREPARED, pe.id.toString(), retryCount);
    }

    return RetryDecision.RETRY;
  }
}
