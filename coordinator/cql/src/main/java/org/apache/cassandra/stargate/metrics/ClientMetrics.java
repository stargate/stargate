/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.stargate.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tags;
import io.netty.buffer.ByteBufAllocatorMetricProvider;
import io.stargate.db.ClientInfo;
import io.stargate.db.metrics.api.ClientInfoMetricsTagProvider;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.stargate.transport.internal.CBUtil;
import org.apache.cassandra.stargate.transport.internal.CqlServer;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ClientMetrics {

  private static final Logger logger = LoggerFactory.getLogger(ClientMetrics.class);

  /** Singleton instance to use. */
  public static final ClientMetrics instance = new ClientMetrics();

  /** Default name factory for the cassandra metrics. */
  private static final DefaultNameFactory factory = new DefaultNameFactory("Client");

  // these are our metric names used
  private static final String REQUESTS_PROCESSED_METRIC;
  private static final String REQUESTS_DISCARDED_METRIC;
  private static final String AUTH_SUCCESS_METRIC;
  private static final String AUTH_FAILURE_METRIC;
  private static final String AUTH_ERROR_METRIC;

  // init to avoid re-computing on each record
  static {
    REQUESTS_PROCESSED_METRIC = metric("RequestsProcessed");
    REQUESTS_DISCARDED_METRIC = metric("RequestDiscarded");
    AUTH_SUCCESS_METRIC = metric("AuthSuccess");
    AUTH_FAILURE_METRIC = metric("AuthFailure");
    AUTH_ERROR_METRIC = metric("AuthError");
  }

  // initialized state
  private volatile boolean initialized = false;
  // internal executor
  private ScheduledExecutorService executorService;

  // dependencies
  private Collection<CqlServer> servers = Collections.emptyList();
  private MeterRegistry meterRegistry;
  private ClientInfoMetricsTagProvider clientInfoTagProvider;

  // internal initialized meters
  private AtomicInteger pausedConnections;
  private Counter totalBytesRead;
  private Counter totalBytesWritten;
  private DistributionSummary bytesReceivedPerFrame;
  private DistributionSummary bytesTransmittedPerFrame;
  private MultiGauge connectedNativeClients;
  private MultiGauge connectedNativeClientsByUser;

  private ClientMetrics() {}

  public void pauseConnection() {
    pausedConnections.incrementAndGet();
  }

  public void unpauseConnection() {
    pausedConnections.decrementAndGet();
  }

  public void incrementTotalBytesRead(double value) {
    totalBytesRead.increment(value);
  }

  public void incrementTotalBytesWritten(double value) {
    totalBytesWritten.increment(value);
  }

  public void recordBytesReceivedPerFrame(double value) {
    bytesReceivedPerFrame.record(value);
  }

  public void recordBytesTransmittedPerFrame(double value) {
    bytesTransmittedPerFrame.record(value);
  }

  public ConnectionMetrics connectionMetrics(ClientInfo clientInfo) {
    if (!initialized) {
      throw new IllegalStateException("Client metrics not initialized yet.");
    }

    return new ConnectionMetricsImpl(clientInfo);
  }

  /**
   * Initializes the {@link ClientMetrics} instance.
   *
   * @param servers List of servers to get metrics for
   * @param meterRegistry Micrometer MeterRegistry to report to
   * @param clientInfoTagProvider Tag provider for the connection
   * @param updatePeriodSeconds The period for internal metric update in 1/s (f.e. for 10 sec rate,
   *     use 0.1). If zero or less task will not be scheduled.
   */
  public synchronized void init(
      Collection<CqlServer> servers,
      MeterRegistry meterRegistry,
      ClientInfoMetricsTagProvider clientInfoTagProvider,
      double updatePeriodSeconds) {

    if (initialized) return;

    this.servers = servers;
    this.meterRegistry = meterRegistry;
    this.clientInfoTagProvider = clientInfoTagProvider;

    // netty gauges
    meterRegistry.gauge(
        metric("NettyDirectMemory"), Tags.empty(), this, ClientMetrics::nettyDirectMemory);
    meterRegistry.gauge(
        metric("NettyHeapMemory"), Tags.empty(), this, ClientMetrics::nettyHeapMemory);

    // connected native clients with the client info tags must be a multi gauge
    connectedNativeClients =
        MultiGauge.builder(metric("connectedNativeClients")).register(meterRegistry);
    connectedNativeClientsByUser =
        MultiGauge.builder(metric("connectedNativeClientsByUser")).register(meterRegistry);

    pausedConnections =
        meterRegistry.gauge(metric("PausedConnections"), Tags.empty(), new AtomicInteger(0));

    totalBytesRead = meterRegistry.counter(metric("TotalBytesRead"));
    totalBytesWritten = meterRegistry.counter(metric("TotalBytesWritten"));

    bytesReceivedPerFrame = meterRegistry.summary(metric("BytesReceivedPerFrame"));
    bytesTransmittedPerFrame = meterRegistry.summary(metric("BytesTransmittedPerFrame"));

    initialized = true;

    // if we have the positive period, init the executor service and submit the update task
    if (updatePeriodSeconds > 0) {
      long fixedRateMillis = Double.valueOf(1000d / updatePeriodSeconds).longValue();
      executorService =
          Executors.newSingleThreadScheduledExecutor(
              new BasicThreadFactory.Builder()
                  .daemon(true)
                  .priority(1)
                  .namingPattern("cql-metrics-updater")
                  .build());
      executorService.scheduleAtFixedRate(
          () -> {
            try {
              updateConnectedClients();
              updateConnectedClientsByUser();
            } catch (Exception e) {
              logger.warn("Error updating the connected client metrics.");
            }
          },
          0,
          fixedRateMillis,
          TimeUnit.MILLISECONDS);
    }
  }

  /** Shuts down the #executor service and marks instance as not initialized. */
  public synchronized void shutdown() {
    if (!initialized) return;

    if (null != executorService && !executorService.isShutdown()) {
      executorService.shutdown();
      try {
        if (!executorService.awaitTermination(1, TimeUnit.SECONDS)) {
          executorService.shutdownNow();
        }
      } catch (InterruptedException e) {
        logger.warn("Interrupted during executor termination, going for shutdownNow().", e);
        executorService.shutdownNow();
      }
    }

    initialized = false;
  }

  private long nettyDirectMemory() {
    if (CBUtil.allocator instanceof ByteBufAllocatorMetricProvider) {
      return ((ByteBufAllocatorMetricProvider) CBUtil.allocator).metric().usedDirectMemory();
    }
    return -1;
  }

  private long nettyHeapMemory() {
    if (CBUtil.allocator instanceof ByteBufAllocatorMetricProvider) {
      return ((ByteBufAllocatorMetricProvider) CBUtil.allocator).metric().usedHeapMemory();
    }
    return -1;
  }

  void updateConnectedClients() {
    Map<Tags, Integer> total = new HashMap<>();

    for (CqlServer server : servers) {
      server
          .countConnectedClientsByConnectionTags()
          .forEach((tags, count) -> total.compute(tags, (t, v) -> v == null ? count : v + count));
    }

    recordMapToMultiGauge(connectedNativeClients, total, Function.identity());
  }

  void updateConnectedClientsByUser() {
    Map<String, Integer> counts = new HashMap<>();

    for (CqlServer server : servers) {
      server
          .countConnectedClientsByUser()
          .forEach(
              (username, count) -> counts.put(username, counts.getOrDefault(username, 0) + count));
    }

    recordMapToMultiGauge(
        connectedNativeClientsByUser, counts, username -> Tags.of("username", username));
  }

  private <T> void recordMapToMultiGauge(
      MultiGauge gauge, Map<T, ? extends Number> source, Function<T, Tags> tagsFunction) {
    List<MultiGauge.Row<?>> rows =
        source.entrySet().stream()
            .map(entry -> MultiGauge.Row.of(tagsFunction.apply(entry.getKey()), entry.getValue()))
            .collect(Collectors.toList());

    gauge.register(rows, true);
  }

  /**
   * Resolves a metric name for the micrometer registry.
   *
   * @param name metric name
   * @return full name
   */
  private static String metric(String name) {
    // to keep the back-ward compatibility, init all metric names with cql. + DefaultNameFactory
    String metricName = factory.createMetricName(name).getMetricName();
    return "cql." + metricName;
  }

  private class ConnectionMetricsImpl implements ConnectionMetrics {

    private final Tags tags;
    private Counter requestsProcessed;
    private final Counter requestsDiscarded;
    private final Counter authSuccess;
    private final Counter authFailure;
    private final Counter authError;

    public ConnectionMetricsImpl(ClientInfo clientInfo) {
      tags =
          Optional.ofNullable(clientInfo)
              .map(clientInfoTagProvider::getClientInfoTags)
              .orElse(Tags.empty());

      Tags tagsByDriver =
          Optional.ofNullable(clientInfo)
              .map(clientInfoTagProvider::getClientInfoTagsByDriver)
              .orElse(Tags.empty());

      requestsProcessed = meterRegistry.counter(REQUESTS_PROCESSED_METRIC, tagsByDriver);
      requestsDiscarded = meterRegistry.counter(REQUESTS_DISCARDED_METRIC, tags);
      authSuccess = meterRegistry.counter(AUTH_SUCCESS_METRIC, tags);
      authFailure = meterRegistry.counter(AUTH_FAILURE_METRIC, tags);
      authError = meterRegistry.counter(AUTH_ERROR_METRIC, tags);
    }

    @Override
    public void markRequestProcessed() {
      requestsProcessed.increment();
    }

    @Override
    public void markRequestDiscarded() {
      requestsDiscarded.increment();
    }

    @Override
    public void markAuthSuccess() {
      authSuccess.increment();
    }

    @Override
    public void markAuthFailure() {
      authFailure.increment();
    }

    @Override
    public void markAuthError() {
      authError.increment();
    }

    @Override
    public void updateDriverInfo(ClientInfo clientInfo) {
      Tags tagsByDriver = clientInfoTagProvider.getClientInfoTagsByDriver(clientInfo);
      requestsProcessed = meterRegistry.counter(REQUESTS_PROCESSED_METRIC, tagsByDriver);
    }

    @Override
    public Tags getTags() {
      return tags;
    }
  }
}
