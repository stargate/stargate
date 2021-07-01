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
package io.stargate.db.tracing;

import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.datastore.DataStoreOptions;
import io.stargate.db.datastore.PersistenceBackedDataStore;
import io.stargate.db.datastore.Row;
import io.stargate.db.query.Predicate;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

public class QueryTracingFetcher {

  private final UUID tracingId;
  private final Persistence.Connection connection;
  private final CompletableFuture<List<Row>> resultFuture = new CompletableFuture<List<Row>>();
  private static final ConsistencyLevel TRACE_CONSISTENCY = ConsistencyLevel.ONE;
  private static final int REQUEST_TRACE_ATTEMPTS = 5;
  private static final Duration TRACE_INTERVAL = Duration.ofMillis(3);
  private final Parameters parameters;
  private final ScheduledExecutorService executorService;
  private final ByteBuffer tracingIdBytes;
  private final PersistenceBackedDataStore persistenceBackedDataStore;

  public QueryTracingFetcher(UUID tracingId, Persistence.Connection connection) {
    this.tracingId = tracingId;
    this.tracingIdBytes = decompose(tracingId);
    this.connection = connection;
    this.parameters = createTracingQueryParameters();
    this.executorService = Executors.newSingleThreadScheduledExecutor();
    this.persistenceBackedDataStore =
        new PersistenceBackedDataStore(connection, DataStoreOptions.defaults());
    querySession(REQUEST_TRACE_ATTEMPTS);
  }

  public CompletionStage<List<Row>> fetch() {
    return resultFuture;
  }

  private Parameters createTracingQueryParameters() {
    return Parameters.builder().consistencyLevel(TRACE_CONSISTENCY).build();
  }

  private void querySession(int remainingAttempts) {
    persistenceBackedDataStore
        .execute(
            persistenceBackedDataStore
                .queryBuilder()
                .select()
                .column("duration", "started_at")
                .from("system_traces", "sessions")
                .where("session_id", Predicate.EQ, tracingId)
                .build()
                .bind())
        .whenComplete(
            (result, error) -> {
              if (error != null) {
                resultFuture.completeExceptionally(error);
              } else {
                if (rowIsNotCorrect(result.rows())) {
                  // Trace is incomplete => fail if last try, or schedule retry
                  if (remainingAttempts == 1) {
                    resultFuture.completeExceptionally(
                        new IllegalStateException(
                            String.format(
                                "Trace %s still not complete after %d attempts",
                                tracingId, REQUEST_TRACE_ATTEMPTS)));
                  } else {
                    executorService.schedule(
                        () -> querySession(remainingAttempts - 1),
                        TRACE_INTERVAL.toNanos(),
                        TimeUnit.NANOSECONDS);
                  }
                } else {
                  queryEvents();
                }
              }
            });
  }

  private boolean rowIsNotCorrect(List<Row> rows) {
    if (rows.isEmpty()) {
      return true;
    }
    Row row = rows.get(0);
    if (row == null) {
      return true;
    }
    return row.isNull("duration") || row.isNull("started_at");
  }

  static ByteBuffer decompose(UUID uuid) {
    long most = uuid.getMostSignificantBits();
    long least = uuid.getLeastSignificantBits();
    byte[] b = new byte[16];

    for (int i = 0; i < 8; ++i) {
      b[i] = (byte) ((int) (most >>> (7 - i) * 8));
      b[8 + i] = (byte) ((int) (least >>> (7 - i) * 8));
    }

    return ByteBuffer.wrap(b);
  }

  private void queryEvents() {
    persistenceBackedDataStore
        .execute(
            persistenceBackedDataStore
                .queryBuilder()
                .select()
                .column("activity", "source", "source_elapsed", "thread")
                .from("system_traces", "events")
                .build()
                .bind())
        .whenComplete(
            (result, error) -> {
              if (error != null) {
                resultFuture.completeExceptionally(error);
              } else {
                resultFuture.complete(result.rows());
              }
            });
  }
}
