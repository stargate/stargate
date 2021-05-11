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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import io.micrometer.core.instrument.*;
import io.netty.buffer.ByteBufAllocatorMetricProvider;
import io.stargate.db.ClientInfo;
import io.stargate.db.metrics.api.ClientInfoMetricsTagProvider;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.stargate.transport.internal.CBUtil;
import org.apache.cassandra.stargate.transport.internal.Server;

public final class ClientMetrics {

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
  private static final String CONNECTED_NATIVE_CLIENTS_METRIC;

  // init to avoid re-computing on each record
  static {
    REQUESTS_PROCESSED_METRIC = metric("RequestsProcessed");
    REQUESTS_DISCARDED_METRIC = metric("RequestDiscarded");
    AUTH_SUCCESS_METRIC = metric("AuthSuccess");
    AUTH_FAILURE_METRIC = metric("AuthFailure");
    AUTH_ERROR_METRIC = metric("AuthError");
    CONNECTED_NATIVE_CLIENTS_METRIC = metric("connectedNativeClients");
  }

  private volatile boolean initialized = false;

  private Collection<Server> servers = Collections.emptyList();
  private MetricRegistry metricRegistry;
  private MeterRegistry meterRegistry;
  private ClientInfoMetricsTagProvider clientInfoTagProvider;

  private AtomicInteger pausedConnections;
  private Counter totalBytesRead;
  private Counter totalBytesWritten;
  private DistributionSummary bytesReceivedPerFrame;
  private DistributionSummary bytesTransmittedPerFrame;
  private MultiGauge connectedNativeClients;

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

  public synchronized void init(
      Collection<Server> servers,
      MetricRegistry metricRegistry,
      MeterRegistry meterRegistry,
      ClientInfoMetricsTagProvider clientInfoTagProvider) {

    if (initialized) return;

    this.servers = servers;
    this.metricRegistry = metricRegistry;
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

    // TODO can we avoid using the timer, but get notified from somebody on the changes?
    Timer connectedNativeClientsTimer = new Timer();
    connectedNativeClientsTimer.schedule(
        new TimerTask() {
          @Override
          public void run() {
            updateConnectedClients();
          }
        },
        10000L,
        10000L);
    registerGauge("connectedNativeClientsByUser", this::countConnectedClientsByUser);

    pausedConnections =
        meterRegistry.gauge(metric("PausedConnections"), Tags.empty(), new AtomicInteger(0));

    totalBytesRead = meterRegistry.counter(metric("TotalBytesRead"));
    totalBytesWritten = meterRegistry.counter(metric("TotalBytesWritten"));

    bytesReceivedPerFrame = meterRegistry.summary(metric("BytesReceivedPerFrame"));
    bytesTransmittedPerFrame = meterRegistry.summary(metric("BytesTransmittedPerFrame"));

    initialized = true;
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
    // first collect all the total count from all servers
    Map<ClientInfo, Integer> total =
        servers.stream()
            .map(Server::countConnectedClientsByClientInfo)
            .reduce(
                new HashMap<>(),
                (combined, single) -> {
                  single.forEach(
                      (clientInfo, count) ->
                          combined.compute(clientInfo, (ci, v) -> v == null ? count : v + count));
                  return combined;
                });

    // then map to multi gauge rows
    List<MultiGauge.Row<?>> rows =
        total.entrySet().stream()
            .map(
                entry -> {
                  Tags clientInfoTags = clientInfoTagProvider.getClientInfoTags(entry.getKey());
                  return MultiGauge.Row.of(clientInfoTags, entry.getValue());
                })
            .collect(Collectors.toList());

    // ensure overwrite is called to re-write existing values to new ones
    connectedNativeClients.register(rows, true);
  }
  private Map<String, Integer> countConnectedClientsByUser() {
    Map<String, Integer> counts = new HashMap<>();

    for (Server server : servers) {
      server
          .countConnectedClientsByUser()
          .forEach(
              (username, count) -> counts.put(username, counts.getOrDefault(username, 0) + count));
    }

    return counts;
  }

  private <T> Gauge<T> registerGauge(String name, Gauge<T> gauge) {
    return metricRegistry.register(factory.createMetricName(name).getMetricName(), gauge);
  }

  /**
   * Resolves a metric name for the micrometer registry.
   *
   * @param name metric name
   * @return full name
   */
  private static String metric(String name) {
    // to keep the back-ward compatibility, init all metric names with cql_ + DefaultNameFactory
    String metricName = factory.createMetricName(name).getMetricName();
    return "cql." + metricName;
  }

  private class ConnectionMetricsImpl implements ConnectionMetrics {

    private final Counter requestsProcessed;
    private final Counter requestsDiscarded;
    private final Counter authSuccess;
    private final Counter authFailure;
    private final Counter authError;
    private final AtomicInteger connectedClients;

    public ConnectionMetricsImpl(ClientInfo clientInfo) {
      Tags tags = Optional.ofNullable(clientInfo)
              .map(clientInfoTagProvider::getClientInfoTags)
              .orElse(Tags.empty());

      requestsProcessed = meterRegistry.counter(REQUESTS_PROCESSED_METRIC, tags);
      requestsDiscarded = meterRegistry.counter(REQUESTS_DISCARDED_METRIC, tags);
      authSuccess = meterRegistry.counter(AUTH_SUCCESS_METRIC, tags);
      authFailure = meterRegistry.counter(AUTH_FAILURE_METRIC, tags);
      authError = meterRegistry.counter(AUTH_ERROR_METRIC, tags);
      connectedClients = meterRegistry.gauge(CONNECTED_NATIVE_CLIENTS_METRIC, tags, new AtomicInteger(0));
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
    public void increaseConnectedNativeClients() {
      connectedClients.incrementAndGet();
    }

    @Override
    public void decreaseConnectedNativeClients() {
      connectedClients.decrementAndGet();
    }

  }

}
