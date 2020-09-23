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
package io.stargate.db.cdc;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.codahale.metrics.MetricRegistry;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.ThreadFactoryBuilder;
import io.stargate.db.metrics.CDCMetrics;
import java.util.concurrent.*;
import org.apache.cassandra.stargate.db.MutationEvent;
import org.apache.cassandra.stargate.exceptions.CDCWriteException;

public final class CDCServiceImpl implements CDCService {

  private final CDCProducer producer;
  private final CDCHealthChecker healthChecker;
  private final CDCConfig config;
  private final ScheduledExecutorService timeoutScheduler =
      Executors.newScheduledThreadPool(
          1, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("failAfter-%d").build());

  private static final CompletableFuture<Void> completedFuture =
      CompletableFuture.completedFuture(null);
  private static final CompletableFuture<Void> unhealthyFuture = new CompletableFuture<>();

  static {
    unhealthyFuture.completeExceptionally(
        new CDCWriteException("CDC producer marked as unhealthy"));
  }

  @VisibleForTesting
  public CDCServiceImpl(
      CDCProducer producer,
      CDCHealthChecker healthChecker,
      CDCConfig config,
      MetricRegistry registry) {
    this.producer = producer;
    this.healthChecker = healthChecker;
    this.config = config;

    // TODO: Decide how to integrate with metrics
    CDCMetrics.instance.init(registry);
  }

  @Override
  public CompletableFuture<Void> publish(MutationEvent mutation) throws CDCWriteException {
    if (!config.isTrackedByCDC(mutation)) {
      return completedFuture;
    }

    if (!healthChecker.isHealthy()) {
      return unhealthyFuture;
    }

    return sendToProducer(mutation);
  }

  private CompletableFuture<Void> sendToProducer(MutationEvent mutation) {
    final long start = System.nanoTime();
    CDCMetrics.instance.incrementInFlight();
    return orTimeout(producer.publish(mutation), config.getLatencyErrorMs())
        .whenComplete(
            (r, e) -> {
              CDCMetrics.instance.decrementInFlight();
              CDCMetrics.instance.updateLatency(System.nanoTime() - start);

              if (e != null) {
                healthChecker.reportSendError(mutation, e);
                CDCMetrics.instance.markProducerFailure();

                if (e instanceof TimeoutException) {
                  CDCMetrics.instance.markProducerTimedOut();
                }
              } else {
                healthChecker.reportSendSuccess();
              }
            });
  }

  /** Equivalent of Java 9+ orTimeout() */
  private <T> CompletableFuture<T> orTimeout(CompletableFuture<T> f, long timeoutMs) {
    if (timeoutMs <= 0) {
      return f;
    }

    try {
      final ScheduledFuture<Boolean> scheduledTimeout =
          timeoutScheduler.schedule(
              () ->
                  f.completeExceptionally(
                      new TimeoutException(String.format("Timed out after %sms", timeoutMs))),
              timeoutMs,
              MILLISECONDS);

      f.thenAccept(r -> scheduledTimeout.cancel(false));
    } catch (Exception ex) {
      // Scheduler was shutdown, nvm
    }

    return f;
  }

  @Override
  public void close() throws Exception {
    healthChecker.close();
    producer.close();
    timeoutScheduler.shutdownNow();
  }

  @VisibleForTesting
  interface CDCConfig {
    boolean isTrackedByCDC(MutationEvent mutation);

    long getLatencyErrorMs();
  }
}
