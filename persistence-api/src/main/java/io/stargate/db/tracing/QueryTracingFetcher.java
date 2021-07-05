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

import com.datastax.oss.driver.internal.core.util.concurrent.BlockingOperation;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.DefaultEventLoopGroup;
import io.stargate.db.Persistence;
import io.stargate.db.datastore.DataStoreOptions;
import io.stargate.db.datastore.PersistenceBackedDataStore;
import io.stargate.db.datastore.Row;
import io.stargate.db.query.Predicate;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

public class QueryTracingFetcher {
  private final UUID tracingId;
  private final ConsistencyLevel consistencyLevel;
  private final CompletableFuture<TracingData> resultFuture = new CompletableFuture<>();
  private static final int REQUEST_TRACE_ATTEMPTS = 5;
  private static final Duration TRACE_INTERVAL = Duration.ofMillis(3);
  private final PersistenceBackedDataStore persistenceBackedDataStore;
  private static final ScheduledExecutorService scheduler = createExecutor();

  public QueryTracingFetcher(
      UUID tracingId, Persistence.Connection connection, ConsistencyLevel consistencyLevel) {
    this.tracingId = tracingId;
    this.consistencyLevel = consistencyLevel;
    this.persistenceBackedDataStore =
        new PersistenceBackedDataStore(connection, DataStoreOptions.defaults());
    querySession(REQUEST_TRACE_ATTEMPTS);
  }

  private static ScheduledExecutorService createExecutor() {
    ThreadFactory safeFactory = new BlockingOperation.SafeThreadFactory();

    ThreadFactory adminThreadFactory =
        new ThreadFactoryBuilder()
            .setThreadFactory(safeFactory)
            .setNameFormat("Query-tracing-fetcher-%d")
            .setDaemon(false)
            .build();
    return new DefaultEventLoopGroup(2, adminThreadFactory);
  }

  public CompletionStage<TracingData> fetch() {
    return resultFuture;
  }

  private void querySession(int remainingAttempts) {
    persistenceBackedDataStore
        .execute(
            persistenceBackedDataStore
                .queryBuilder()
                .select()
                .column("duration", "started_at", "request")
                .from("system_traces", "sessions")
                .where("session_id", Predicate.EQ, tracingId)
                .build()
                .bind(),
            consistencyLevel)
        .whenComplete(
            (result, error) -> {
              if (error != null) {
                resultFuture.completeExceptionally(error);
              } else {
                Row row = result.one();
                if (rowIsNotCorrect(row)) {
                  // Trace is incomplete => fail if last try, or schedule retry
                  if (remainingAttempts == 1) {
                    resultFuture.completeExceptionally(
                        new IllegalStateException(
                            String.format(
                                "Trace %s still not complete after %d attempts",
                                tracingId, REQUEST_TRACE_ATTEMPTS)));
                  } else {
                    scheduler.schedule(
                        () -> querySession(remainingAttempts - 1),
                        TRACE_INTERVAL.toNanos(),
                        TimeUnit.NANOSECONDS);
                  }
                } else {
                  queryEvents(row);
                }
              }
            });
  }

  private boolean rowIsNotCorrect(Row row) {
    if (row == null) {
      return true;
    }
    return row.isNull("duration") || row.isNull("started_at");
  }

  private void queryEvents(Row session) {
    persistenceBackedDataStore
        .execute(
            persistenceBackedDataStore
                .queryBuilder()
                .select()
                .column("activity", "source", "source_elapsed", "thread", "event_id")
                .from("system_traces", "events")
                .build()
                .bind(),
            consistencyLevel)
        .whenComplete(
            (result, error) -> {
              if (error != null) {
                resultFuture.completeExceptionally(error);
              } else {
                resultFuture.complete(new TracingData(result.rows(), session));
              }
            });
  }
}
