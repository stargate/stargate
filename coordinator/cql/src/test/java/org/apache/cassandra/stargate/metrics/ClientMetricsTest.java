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
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.cassandra.stargate.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.stargate.db.ClientInfo;
import io.stargate.db.DriverInfo;
import io.stargate.db.metrics.api.ClientInfoMetricsTagProvider;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.cassandra.stargate.transport.internal.CqlServer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ClientMetricsTest {

  ClientMetrics clientMetrics = ClientMetrics.instance;

  MeterRegistry meterRegistry = new SimpleMeterRegistry();

  // manual mocks, init before all
  ClientInfoMetricsTagProvider clientTagProvider;
  CqlServer server1;
  CqlServer server2;
  ClientInfo clientInfo1;
  ClientInfo clientInfo2;
  DriverInfo driverInfo1;
  DriverInfo driverInfo2;

  @BeforeAll
  public void initMocks() {
    clientTagProvider = mock(ClientInfoMetricsTagProvider.class);
    server1 = mock(CqlServer.class);
    server2 = mock(CqlServer.class);
    clientInfo1 = mock(ClientInfo.class);
    clientInfo2 = mock(ClientInfo.class);
    driverInfo1 = mock(DriverInfo.class);
    driverInfo2 = mock(DriverInfo.class);
    when(driverInfo1.name()).thenReturn("driver1");
    when(driverInfo1.version()).thenReturn(Optional.of("4.15.0"));
    when(driverInfo2.name()).thenReturn("driver2");
    when(clientInfo1.driverInfo()).thenReturn(Optional.of(driverInfo1));
    when(clientInfo2.driverInfo()).thenReturn(Optional.of(driverInfo2));
    when(clientTagProvider.getClientInfoTags(clientInfo1))
        .thenReturn(Tags.of("driverName", "driver1", "driverVersion", "4.15.0"));
    when(clientTagProvider.getClientInfoTags(clientInfo2))
        .thenReturn(Tags.of("driverName", "driver2", "driverVersion", "unknown"));

    List<CqlServer> servers = Arrays.asList(server1, server2);
    clientMetrics.init(servers, meterRegistry, clientTagProvider, 0d);
  }

  @Nested
  class MarkRequestProcessed {

    @Test
    public void happyPath() {
      clientMetrics.connectionMetrics(clientInfo1).markRequestProcessed();
      clientMetrics.connectionMetrics(clientInfo1).markRequestProcessed();
      clientMetrics.connectionMetrics(clientInfo2).markRequestProcessed();

      Counter c1 =
          meterRegistry
              .get("cql.org.apache.cassandra.metrics.Client.RequestsProcessed")
              .tag("driverName", "driver1")
              .tag("driverVersion", "4.15.0")
              .counter();

      assertThat(c1.count()).isEqualTo(2d);

      Counter c2 =
          meterRegistry
              .get("cql.org.apache.cassandra.metrics.Client.RequestsProcessed")
              .tag("driverName", "driver2")
              .tag("driverVersion", "unknown")
              .counter();

      assertThat(c2.count()).isEqualTo(1d);
    }
  }

  @Nested
  class MarkRequestDiscarded {

    @Test
    public void happyPath() {
      clientMetrics.connectionMetrics(clientInfo1).markRequestDiscarded();
      clientMetrics.connectionMetrics(clientInfo1).markRequestDiscarded();
      clientMetrics.connectionMetrics(clientInfo2).markRequestDiscarded();

      Counter c1 =
          meterRegistry
              .get("cql.org.apache.cassandra.metrics.Client.RequestDiscarded")
              .tag("driverName", "driver1")
              .tag("driverVersion", "4.15.0")
              .counter();

      assertThat(c1.count()).isEqualTo(2d);

      Counter c2 =
          meterRegistry
              .get("cql.org.apache.cassandra.metrics.Client.RequestDiscarded")
              .tag("driverName", "driver2")
              .tag("driverVersion", "unknown")
              .counter();

      assertThat(c2.count()).isEqualTo(1d);
    }
  }

  @Nested
  class MarkAuthSuccess {

    @Test
    public void happyPath() {
      clientMetrics.connectionMetrics(clientInfo1).markAuthSuccess();
      clientMetrics.connectionMetrics(clientInfo1).markAuthSuccess();
      clientMetrics.connectionMetrics(clientInfo2).markAuthSuccess();

      Counter c1 =
          meterRegistry
              .get("cql.org.apache.cassandra.metrics.Client.AuthSuccess")
              .tag("driverName", "driver1")
              .tag("driverVersion", "4.15.0")
              .counter();

      assertThat(c1.count()).isEqualTo(2d);

      Counter c2 =
          meterRegistry
              .get("cql.org.apache.cassandra.metrics.Client.AuthSuccess")
              .tag("driverName", "driver2")
              .tag("driverVersion", "unknown")
              .counter();

      assertThat(c2.count()).isEqualTo(1d);
    }
  }

  @Nested
  class MarkAuthFailure {

    @Test
    public void happyPath() {
      clientMetrics.connectionMetrics(clientInfo1).markAuthFailure();
      clientMetrics.connectionMetrics(clientInfo1).markAuthFailure();
      clientMetrics.connectionMetrics(clientInfo2).markAuthFailure();

      Counter c1 =
          meterRegistry
              .get("cql.org.apache.cassandra.metrics.Client.AuthFailure")
              .tag("driverName", "driver1")
              .tag("driverVersion", "4.15.0")
              .counter();

      assertThat(c1.count()).isEqualTo(2d);

      Counter c2 =
          meterRegistry
              .get("cql.org.apache.cassandra.metrics.Client.AuthFailure")
              .tag("driverName", "driver2")
              .tag("driverVersion", "unknown")
              .counter();

      assertThat(c2.count()).isEqualTo(1d);
    }
  }

  @Nested
  class MarkAuthError {

    @Test
    public void happyPath() {
      clientMetrics.connectionMetrics(clientInfo1).markAuthError();
      clientMetrics.connectionMetrics(clientInfo1).markAuthError();
      clientMetrics.connectionMetrics(clientInfo2).markAuthError();

      Counter c1 =
          meterRegistry
              .get("cql.org.apache.cassandra.metrics.Client.AuthError")
              .tag("driverName", "driver1")
              .tag("driverVersion", "4.15.0")
              .counter();

      assertThat(c1.count()).isEqualTo(2d);

      Counter c2 =
          meterRegistry
              .get("cql.org.apache.cassandra.metrics.Client.AuthError")
              .tag("driverName", "driver2")
              .tag("driverVersion", "unknown")
              .counter();

      assertThat(c2.count()).isEqualTo(1d);
    }
  }

  @Nested
  class PauseConnections {

    @Test
    public void happyPath() {
      clientMetrics.pauseConnection();
      clientMetrics.pauseConnection();

      Gauge g1 =
          meterRegistry.get("cql.org.apache.cassandra.metrics.Client.PausedConnections").gauge();

      assertThat(g1.value()).isEqualTo(2d);

      clientMetrics.unpauseConnection();

      Gauge g2 =
          meterRegistry.get("cql.org.apache.cassandra.metrics.Client.PausedConnections").gauge();

      assertThat(g2.value()).isEqualTo(1d);
    }
  }

  @Nested
  class IncrementTotalBytesRead {

    @Test
    public void happyPath() {
      clientMetrics.incrementTotalBytesRead(11);
      clientMetrics.incrementTotalBytesRead(22);

      Counter c1 =
          meterRegistry.get("cql.org.apache.cassandra.metrics.Client.TotalBytesRead").counter();

      assertThat(c1.count()).isEqualTo(33);
    }
  }

  @Nested
  class IncrementTotalBytesWritten {

    @Test
    public void happyPath() {
      clientMetrics.incrementTotalBytesWritten(22);
      clientMetrics.incrementTotalBytesWritten(33);

      Counter c1 =
          meterRegistry.get("cql.org.apache.cassandra.metrics.Client.TotalBytesWritten").counter();

      assertThat(c1.count()).isEqualTo(55);
    }
  }

  @Nested
  class RecordBytesReceivedPerFrame {

    @Test
    public void happyPath() {
      clientMetrics.recordBytesReceivedPerFrame(11);
      clientMetrics.recordBytesReceivedPerFrame(33);

      DistributionSummary summary =
          meterRegistry
              .get("cql.org.apache.cassandra.metrics.Client.BytesReceivedPerFrame")
              .summary();

      assertThat(summary.count()).isEqualTo(2);
      assertThat(summary.totalAmount()).isEqualTo(44);
    }
  }

  @Nested
  class RecordBytesTransmittedPerFrame {

    @Test
    public void happyPath() {
      clientMetrics.recordBytesTransmittedPerFrame(22);
      clientMetrics.recordBytesTransmittedPerFrame(44);

      DistributionSummary summary =
          meterRegistry
              .get("cql.org.apache.cassandra.metrics.Client.BytesTransmittedPerFrame")
              .summary();

      assertThat(summary.count()).isEqualTo(2);
      assertThat(summary.totalAmount()).isEqualTo(66);
    }
  }

  @Nested
  class UpdateConnectedClients {

    @Test
    public void happyPath() {
      Map<Tags, Integer> clientInfoTagsMap = new HashMap<>();
      clientInfoTagsMap.put(Tags.of("cm", "one"), 10);
      clientInfoTagsMap.put(Tags.of("cm", "two"), 20);

      when(server1.countConnectedClientsByConnectionTags()).thenReturn(clientInfoTagsMap);
      when(server2.countConnectedClientsByConnectionTags()).thenReturn(clientInfoTagsMap);

      clientMetrics.updateConnectedClients();

      Gauge g1 =
          meterRegistry
              .get("cql.org.apache.cassandra.metrics.Client.connectedNativeClients")
              .tag("cm", "one")
              .gauge();

      assertThat(g1.value()).isEqualTo(20);

      Gauge g2 =
          meterRegistry
              .get("cql.org.apache.cassandra.metrics.Client.connectedNativeClients")
              .tag("cm", "two")
              .gauge();

      assertThat(g2.value()).isEqualTo(40);
    }

    @Test
    public void removedConnections() {
      Map<Tags, Integer> clientInfoTagsMap = new HashMap<>();
      clientInfoTagsMap.put(Tags.of("cm", "one"), 10);
      clientInfoTagsMap.put(Tags.of("cm", "two"), 20);
      when(server1.countConnectedClientsByConnectionTags()).thenReturn(clientInfoTagsMap);
      when(server2.countConnectedClientsByConnectionTags()).thenReturn(clientInfoTagsMap);

      clientMetrics.updateConnectedClients();

      when(server1.countConnectedClientsByConnectionTags()).thenReturn(clientInfoTagsMap);
      when(server2.countConnectedClientsByConnectionTags()).thenReturn(Collections.emptyMap());

      clientMetrics.updateConnectedClients();

      Gauge g1 =
          meterRegistry
              .get("cql.org.apache.cassandra.metrics.Client.connectedNativeClients")
              .tag("cm", "one")
              .gauge();

      assertThat(g1.value()).isEqualTo(10);

      Gauge g2 =
          meterRegistry
              .get("cql.org.apache.cassandra.metrics.Client.connectedNativeClients")
              .tag("cm", "two")
              .gauge();

      assertThat(g2.value()).isEqualTo(20);
    }

    @Test
    public void noConnections() {
      Map<Tags, Integer> clientInfoTagsMap = new HashMap<>();
      clientInfoTagsMap.put(Tags.of("cm", "one"), 10);
      clientInfoTagsMap.put(Tags.of("cm", "two"), 20);
      when(server1.countConnectedClientsByConnectionTags()).thenReturn(clientInfoTagsMap);
      when(server2.countConnectedClientsByConnectionTags()).thenReturn(clientInfoTagsMap);

      clientMetrics.updateConnectedClients();

      when(server1.countConnectedClientsByConnectionTags()).thenReturn(Collections.emptyMap());
      when(server2.countConnectedClientsByConnectionTags()).thenReturn(Collections.emptyMap());

      clientMetrics.updateConnectedClients();

      Throwable throwable =
          catchThrowable(
              () ->
                  meterRegistry
                      .get("cql.org.apache.cassandra.metrics.Client.connectedNativeClients")
                      .gauges());

      assertThat(throwable).isInstanceOf(MeterNotFoundException.class);
    }
  }

  @Nested
  class UpdateConnectedClientsByUsers {

    @Test
    public void happyPath() {
      Map<String, Integer> userConnectionMap = new HashMap<>();
      userConnectionMap.put("user1", 10);
      userConnectionMap.put("user2", 20);

      when(server1.countConnectedClientsByUser()).thenReturn(userConnectionMap);
      when(server2.countConnectedClientsByUser()).thenReturn(userConnectionMap);

      clientMetrics.updateConnectedClientsByUser();

      Gauge g1 =
          meterRegistry
              .get("cql.org.apache.cassandra.metrics.Client.connectedNativeClientsByUser")
              .tag("username", "user1")
              .gauge();

      assertThat(g1.value()).isEqualTo(20);

      Gauge g2 =
          meterRegistry
              .get("cql.org.apache.cassandra.metrics.Client.connectedNativeClientsByUser")
              .tag("username", "user2")
              .gauge();

      assertThat(g2.value()).isEqualTo(40);
    }

    @Test
    public void updatedUsers() {
      Map<String, Integer> userConnectionMap = new HashMap<>();
      userConnectionMap.put("user1", 10);
      userConnectionMap.put("user2", 10);

      when(server1.countConnectedClientsByUser()).thenReturn(userConnectionMap);
      when(server2.countConnectedClientsByUser()).thenReturn(userConnectionMap);

      clientMetrics.updateConnectedClientsByUser();

      userConnectionMap.put("user1", 20);
      userConnectionMap.remove("user2");

      clientMetrics.updateConnectedClientsByUser();

      Gauge g1 =
          meterRegistry
              .get("cql.org.apache.cassandra.metrics.Client.connectedNativeClientsByUser")
              .tag("username", "user1")
              .gauge();

      assertThat(g1.value()).isEqualTo(40);

      Throwable throwable =
          catchThrowable(
              () ->
                  meterRegistry
                      .get("cql.org.apache.cassandra.metrics.Client.connectedNativeClientsByUser")
                      .tag("username", "user2")
                      .gauge());

      assertThat(throwable).isInstanceOf(MeterNotFoundException.class);
    }
  }

  @Nested
  class State {

    @Test
    public void nettyDirectMemory() {
      Gauge gauge =
          meterRegistry.get("cql.org.apache.cassandra.metrics.Client.NettyDirectMemory").gauge();

      assertThat(gauge).isNotNull();
    }

    @Test
    public void nettyHeapMemory() {
      Gauge gauge =
          meterRegistry.get("cql.org.apache.cassandra.metrics.Client.NettyHeapMemory").gauge();

      assertThat(gauge).isNotNull();
    }
  }
}
