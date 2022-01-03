/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.stargate.exceptions;

import static java.lang.Math.max;

public enum RequestFailureReason {
  UNKNOWN(0),
  READ_TOO_MANY_TOMBSTONES(1),
  TIMEOUT(2),
  INCOMPATIBLE_SCHEMA(3),

  /** The request queried an index but that index wasn't build on the data node. */
  INDEX_NOT_AVAILABLE,

  /**
   * The request was writing some data on a CDC enabled table but the CDC commit log segment doesn't
   * have space anymore (slow CDC consumer).
   */
  CDC_SEGMENT_FULL,

  /**
   * We executed a forwarded counter write but got a failure (any {@link RequestExecutionException}
   * that is not a timeout or an unavailable exception; typically a {@link WriteFailureException} or
   * the like).
   */
  COUNTER_FORWARDING_FAILURE,

  /**
   * We didn't find the table for an operation on a replica. This almost surely implies a race
   * between the operation and either creation or drop of the table (or, possibly, of the keyspace
   * containing that table, see comment on {@link #UNKNOWN_KEYSPACE}).
   */
  UNKNOWN_TABLE,

  /**
   * We didn't find the keyspace for an operation on a replica. This almost surely implies a race
   * between the operation and either creation or drop of the keyspace.
   *
   * <p>Note however that in many cases, the absence of a keyspace will show up as {@link
   * #UNKNOWN_TABLE}, so the difference between the 2 reason should not be relied upon too strongly.
   */
  UNKNOWN_KEYSPACE,

  /**
   * We didn't find a column for an operation on a replica. This almost surely implies a race
   * between the operation and either creation or drop of the column.
   */
  UNKNOWN_COLUMN,

  /** NodeSync service is not running. */
  NODESYNC_NOT_RUNNING,

  /** We didn't find the NodeSync user validation on a replica. */
  UNKNOWN_NODESYNC_USER_VALIDATION,

  /** A NodeSync user validation is cancelled on a replica. */
  CANCELLED_NODESYNC_USER_VALIDATION,

  /** We can't enable NodeSync tracing on a replica because it's already enabled. */
  NODESYNC_TRACING_ALREADY_ENABLED,

  /**
   * A mutation to be sent for read-repair is oversize (cannot fit in the commit log) and cannot be
   * broken up further.
   */
  OVERSIZE_READ_REPAIR_MUTATION,

  /**
   * A read request that failed because the buffer pool is exhausted. The chunk cache has an
   * overhead for in-flight reads that should ensure that the buffer pool is never exhausted.
   * However, if this overhead is not sufficiently large for the amount of in-flight reads, then it
   * might happen that a read request fails due to the pool being exhausted.
   */
  BUFFER_POOL_EXHAUSTED,

  /**
   * Backup service is not running.
   */
  BACKUP_SERVICE_NOT_RUNNING,

  /**
   * The file could not be uploaded or downloaded from remote storage.
   */
  REMOTE_STORAGE_FAILURE,

  /**
   * The node is still bootstrapping and is therefore not ready to serve read requests.
   */
  BOOTSTRAPPING,

  /**
   * Used when receiving a code we do not know to indicate that it is a reason added in newer
   * version than us, but is something somewhat expected by that node from the future (it is not
   * {@link #UNKNOWN}).
   */
  FROM_THE_FUTURE;

  public final int code;
  private final boolean hasProtocolSupport;

  /** Enum values that have a failure code can be represented in (some of) OSS protocol versions. */
  RequestFailureReason(int code) {
    this.code = code;
    this.hasProtocolSupport = true;
  }

  /**
   * Enum values without a failure code will be represented as {@link #UNKNOWN} at the protocol
   * level. These failure reasons have dedicated enum values so that they could be meaningfully
   * represented in Stargate internal exceptions and log messages.
   */
  RequestFailureReason() {
    this.code = 0; // UNKNOWN
    this.hasProtocolSupport = false;
  }

  private static final RequestFailureReason[] codeToReasonMap;

  static {
    RequestFailureReason[] reasons = values();

    int max = -1;
    for (RequestFailureReason r : reasons) {
      max = max(r.code, max);
    }

    RequestFailureReason[] codeMap = new RequestFailureReason[max + 1];

    for (RequestFailureReason reason : reasons) {
      if (!reason.hasProtocolSupport) {
        continue;
      }

      if (codeMap[reason.code] != null) {
        throw new RuntimeException(
            "Two RequestFailureReason-s that map to the same code: " + reason.code);
      }
      codeMap[reason.code] = reason;
    }

    codeToReasonMap = codeMap;
  }

  public static RequestFailureReason fromCode(int code) {
    if (code < 0) {
      throw new IllegalArgumentException(
          "RequestFailureReason code must be non-negative (got " + code + ')');
    }

    // be forgiving and return UNKNOWN if we aren't aware of the code - for forward compatibility
    return code < codeToReasonMap.length ? codeToReasonMap[code] : UNKNOWN;
  }
}
