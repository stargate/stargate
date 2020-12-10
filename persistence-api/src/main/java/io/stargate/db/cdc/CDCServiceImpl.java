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
import io.stargate.config.store.api.ConfigStore;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.db.cdc.api.MutationEvent;
import io.stargate.db.cdc.config.CDCConfig;
import io.stargate.db.cdc.config.CDCConfigLoader;
import io.stargate.db.metrics.CDCMetrics;
import java.util.concurrent.*;
import org.apache.cassandra.stargate.exceptions.CDCWriteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CDCServiceImpl implements CDCService {

  private static final CompletableFuture<Void> completedFuture =
      CompletableFuture.completedFuture(null);
  private static final CompletableFuture<Void> unhealthyFuture = new CompletableFuture<>();
  private static final Logger logger = LoggerFactory.getLogger(CDCService.class);

  private final CDCProducer producer;
  private final CDCHealthChecker healthChecker;
  private final CDCConfig config;
  private final ScheduledExecutorService timeoutScheduler =
      Executors.newScheduledThreadPool(
          1, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("failAfter-%d").build());

  static {
    unhealthyFuture.completeExceptionally(
        new CDCWriteException("CDC producer marked as unhealthy"));
  }

  public CDCServiceImpl(CDCProducer producer, Metrics metrics, ConfigStore configStore) {
    this(producer, CDCConfigLoader.loadConfig(configStore), metrics.getRegistry("cdc"));
  }

  private CDCServiceImpl(CDCProducer producer, CDCConfig config, MetricRegistry registry) {
    this(
        producer,
        new DefaultCDCHealthChecker(
            config.getErrorRateThreshold(),
            config.getMinErrorsPerSecond(),
            config.getEWMAIntervalMinutes()),
        config,
        registry);
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

    CDCMetrics.instance.init(registry);
  }

  @Override
  public CompletableFuture<Void> publish(MutationEvent mutation) {
    if (!config.isTrackedByCDC(mutation.table())) {
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
    return orTimeout(producer.publish(mutation), config.getProducerTimeoutMs())
        .handle(
            (r, e) -> {
              CDCMetrics.instance.decrementInFlight();
              CDCMetrics.instance.updateLatency(System.nanoTime() - start);

              if (e != null) {
                healthChecker.reportSendError();
                CDCMetrics.instance.markProducerFailure();

                if (e instanceof TimeoutException) {
                  CDCMetrics.instance.markProducerTimedOut();
                }

                throw new CDCWriteException(
                    "There was an error sending to the producer: " + e.getMessage(), e);
              }

              healthChecker.reportSendSuccess();
              return r;
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
      logger.warn("Scheduler shutdown", ex);
    }

    return f;
  }

  @Override
  public void close() throws Exception {
    healthChecker.close();
    timeoutScheduler.shutdownNow();
    try {
      producer.close().get();
    } catch (Exception e) {
      logger.info("There was an issue releasing resources of CDC Producer", e);
    }
  }
}
