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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import io.netty.buffer.ByteBufAllocatorMetricProvider;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.MetricNameFactory;
import org.apache.cassandra.stargate.transport.internal.CBUtil;
import org.apache.cassandra.stargate.transport.internal.ClientStat;
import org.apache.cassandra.stargate.transport.internal.ConnectedClient;
import org.apache.cassandra.stargate.transport.internal.Server;

public final class ClientMetrics {
  public static final ClientMetrics instance = new ClientMetrics();

  private static final MetricNameFactory factory = new DefaultNameFactory("Client");

  private volatile boolean initialized = false;
  private Collection<Server> servers = Collections.emptyList();
  private MetricRegistry metricRegistry;

  private Meter authSuccess;
  private Meter authFailure;
  private Meter authError;

  private AtomicInteger pausedConnections;
  private Meter requestDiscarded;

  private Counter totalBytesRead;
  private Counter totalBytesWritten;
  private Histogram bytesReceivedPerFrame;
  private Histogram bytesTransmittedPerFrame;

  private ClientMetrics() {}

  public void markAuthSuccess() {
    authSuccess.mark();
  }

  public void markAuthFailure() {
    authFailure.mark();
  }

  public void markAuthError() {
    authError.mark();
  }

  public void pauseConnection() {
    pausedConnections.incrementAndGet();
  }

  public void unpauseConnection() {
    pausedConnections.decrementAndGet();
  }

  public void markRequestDiscarded() {
    requestDiscarded.mark();
  }

  public Counter getTotalBytesRead() {
    return totalBytesRead;
  }

  public Counter getTotalBytesWritten() {
    return totalBytesWritten;
  }

  public Histogram getBytesReceivedPerFrame() {
    return bytesReceivedPerFrame;
  }

  public Histogram getBytesTransmittedPerFrame() {
    return bytesTransmittedPerFrame;
  }

  public List<ConnectedClient> allConnectedClients() {
    List<ConnectedClient> clients = new ArrayList<>();

    for (Server server : servers) clients.addAll(server.getConnectedClients());

    return clients;
  }

  public synchronized void init(Collection<Server> servers, MetricRegistry metricRegistry) {
    if (initialized) return;

    this.servers = servers;
    this.metricRegistry = metricRegistry;

    registerGauge("NettyDirectMemory", this::nettyDirectMemory);
    registerGauge("NettyHeapMemory", this::nettyHeapMemory);

    registerGauge("connectedNativeClients", this::countConnectedClients);
    registerGauge("connectedNativeClientsByUser", this::countConnectedClientsByUser);
    registerGauge("connections", this::connectedClients);
    registerGauge("clientsByProtocolVersion", this::recentClientStats);

    authSuccess = registerMeter("AuthSuccess");
    authFailure = registerMeter("AuthFailure");
    authError = registerMeter("AuthError");

    pausedConnections = new AtomicInteger();
    registerGauge("PausedConnections", pausedConnections::get);
    requestDiscarded = registerMeter("RequestDiscarded");

    totalBytesRead = registerCounter("TotalBytesRead");
    totalBytesWritten = registerCounter("TotalBytesWritten");

    bytesReceivedPerFrame = registerHistogram("BytesReceivedPerFrame");
    bytesTransmittedPerFrame = registerHistogram("BytesTransmittedPerFrame");

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

  private int countConnectedClients() {
    int count = 0;

    for (Server server : servers) count += server.countConnectedClients();

    return count;
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

  private List<Map<String, String>> connectedClients() {
    List<Map<String, String>> clients = new ArrayList<>();

    for (Server server : servers)
      for (ConnectedClient client : server.getConnectedClients()) clients.add(client.asMap());

    return clients;
  }

  private List<Map<String, String>> recentClientStats() {
    List<Map<String, String>> stats = new ArrayList<>();

    for (Server server : servers)
      for (ClientStat stat : server.recentClientStats()) stats.add(stat.asMap());

    stats.sort(Comparator.comparing(map -> map.get(ClientStat.PROTOCOL_VERSION)));

    return stats;
  }

  private <T> Gauge<T> registerGauge(String name, Gauge<T> gauge) {
    return metricRegistry.register(factory.createMetricName(name).getMetricName(), gauge);
  }

  private Histogram registerHistogram(String name) {
    return metricRegistry.histogram(factory.createMetricName(name).getMetricName());
  }

  private Counter registerCounter(String name) {
    return metricRegistry.counter(factory.createMetricName(name).getMetricName());
  }

  private Meter registerMeter(String name) {
    return metricRegistry.meter(factory.createMetricName(name).getMetricName());
  }
}
