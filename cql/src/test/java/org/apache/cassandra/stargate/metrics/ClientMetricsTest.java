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

import com.codahale.metrics.MetricRegistry;
import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.stargate.db.ClientInfo;
import io.stargate.db.metrics.api.ClientInfoMetricsTagProvider;
import java.util.*;
import org.apache.cassandra.stargate.transport.internal.Server;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ClientMetricsTest {

  ClientMetrics clientMetrics = ClientMetrics.instance;

  MeterRegistry meterRegistry = new SimpleMeterRegistry();

  // manual mocks, init before all
  MetricRegistry metricRegistry; // TODO Remove when possible
  ClientInfoMetricsTagProvider clientTagProvider;
  Server server1;
  Server server2;
  ClientInfo clientInfo1;
  ClientInfo clientInfo2;

  @BeforeAll
  public void initMocks() {
    metricRegistry = mock(MetricRegistry.class);
    clientTagProvider = mock(ClientInfoMetricsTagProvider.class);
    server1 = mock(Server.class);
    server2 = mock(Server.class);
    clientInfo1 = mock(ClientInfo.class);
    clientInfo2 = mock(ClientInfo.class);

    when(clientTagProvider.getClientInfoTags(clientInfo1)).thenReturn(Tags.of("client", "one"));
    when(clientTagProvider.getClientInfoTags(clientInfo2)).thenReturn(Tags.of("client", "two"));

    List<Server> servers = Arrays.asList(server1, server2);
    clientMetrics.init(servers, metricRegistry, meterRegistry, clientTagProvider);
  }

  @Nested
  class MarkRequestProcessed {

    @Test
    public void happyPath() {
      clientMetrics.markRequestProcessed(clientInfo1);
      clientMetrics.markRequestProcessed(clientInfo1);
      clientMetrics.markRequestProcessed(clientInfo2);

      Counter c1 =
          meterRegistry
              .get("cql.org.apache.cassandra.metrics.Client.RequestsProcessed")
              .tag("client", "one")
              .counter();

      assertThat(c1.count()).isEqualTo(2d);

      Counter c2 =
          meterRegistry
              .get("cql.org.apache.cassandra.metrics.Client.RequestsProcessed")
              .tag("client", "two")
              .counter();

      assertThat(c2.count()).isEqualTo(1d);
    }
  }

  @Nested
  class MarkRequestDiscarded {

    @Test
    public void happyPath() {
      clientMetrics.markRequestDiscarded(clientInfo1);
      clientMetrics.markRequestDiscarded(clientInfo1);
      clientMetrics.markRequestDiscarded(clientInfo2);

      Counter c1 =
          meterRegistry
              .get("cql.org.apache.cassandra.metrics.Client.RequestDiscarded")
              .tag("client", "one")
              .counter();

      assertThat(c1.count()).isEqualTo(2d);

      Counter c2 =
          meterRegistry
              .get("cql.org.apache.cassandra.metrics.Client.RequestDiscarded")
              .tag("client", "two")
              .counter();

      assertThat(c2.count()).isEqualTo(1d);
    }
  }

  @Nested
  class MarkAuthSuccess {

    @Test
    public void happyPath() {
      clientMetrics.markAuthSuccess(clientInfo1);
      clientMetrics.markAuthSuccess(clientInfo1);
      clientMetrics.markAuthSuccess(clientInfo2);

      Counter c1 =
          meterRegistry
              .get("cql.org.apache.cassandra.metrics.Client.AuthSuccess")
              .tag("client", "one")
              .counter();

      assertThat(c1.count()).isEqualTo(2d);

      Counter c2 =
          meterRegistry
              .get("cql.org.apache.cassandra.metrics.Client.AuthSuccess")
              .tag("client", "two")
              .counter();

      assertThat(c2.count()).isEqualTo(1d);
    }
  }

  @Nested
  class MarkAuthFailure {

    @Test
    public void happyPath() {
      clientMetrics.markAuthFailure(clientInfo1);
      clientMetrics.markAuthFailure(clientInfo1);
      clientMetrics.markAuthFailure(clientInfo2);

      Counter c1 =
          meterRegistry
              .get("cql.org.apache.cassandra.metrics.Client.AuthFailure")
              .tag("client", "one")
              .counter();

      assertThat(c1.count()).isEqualTo(2d);

      Counter c2 =
          meterRegistry
              .get("cql.org.apache.cassandra.metrics.Client.AuthFailure")
              .tag("client", "two")
              .counter();

      assertThat(c2.count()).isEqualTo(1d);
    }
  }

  @Nested
  class MarkAuthError {

    @Test
    public void happyPath() {
      clientMetrics.markAuthError(clientInfo1);
      clientMetrics.markAuthError(clientInfo1);
      clientMetrics.markAuthError(clientInfo2);

      Counter c1 =
          meterRegistry
              .get("cql.org.apache.cassandra.metrics.Client.AuthError")
              .tag("client", "one")
              .counter();

      assertThat(c1.count()).isEqualTo(2d);

      Counter c2 =
          meterRegistry
              .get("cql.org.apache.cassandra.metrics.Client.AuthError")
              .tag("client", "two")
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
      Map<ClientInfo, Integer> clientInfoConnectionMap = new HashMap<>();
      clientInfoConnectionMap.put(clientInfo1, 10);
      clientInfoConnectionMap.put(clientInfo2, 20);
      when(server1.countConnectedClientsByClientInfo()).thenReturn(clientInfoConnectionMap);
      when(server2.countConnectedClientsByClientInfo()).thenReturn(clientInfoConnectionMap);

      clientMetrics.updateConnectedClients();

      Gauge g1 =
          meterRegistry
              .get("cql.org.apache.cassandra.metrics.Client.connectedNativeClients")
              .tag("client", "one")
              .gauge();

      assertThat(g1.value()).isEqualTo(20);

      Gauge g2 =
          meterRegistry
              .get("cql.org.apache.cassandra.metrics.Client.connectedNativeClients")
              .tag("client", "two")
              .gauge();

      assertThat(g2.value()).isEqualTo(40);
    }

    @Test
    public void removedConnections() {
      Map<ClientInfo, Integer> clientInfoConnectionMap = new HashMap<>();
      clientInfoConnectionMap.put(clientInfo1, 10);
      clientInfoConnectionMap.put(clientInfo2, 20);
      when(server1.countConnectedClientsByClientInfo()).thenReturn(clientInfoConnectionMap);
      when(server2.countConnectedClientsByClientInfo()).thenReturn(clientInfoConnectionMap);

      clientMetrics.updateConnectedClients();

      when(server1.countConnectedClientsByClientInfo()).thenReturn(clientInfoConnectionMap);
      when(server2.countConnectedClientsByClientInfo()).thenReturn(Collections.emptyMap());

      clientMetrics.updateConnectedClients();

      Gauge g1 =
          meterRegistry
              .get("cql.org.apache.cassandra.metrics.Client.connectedNativeClients")
              .tag("client", "one")
              .gauge();

      assertThat(g1.value()).isEqualTo(10);

      Gauge g2 =
          meterRegistry
              .get("cql.org.apache.cassandra.metrics.Client.connectedNativeClients")
              .tag("client", "two")
              .gauge();

      assertThat(g2.value()).isEqualTo(20);
    }

    @Test
    public void noConnections() {
      Map<ClientInfo, Integer> clientInfoConnectionMap = new HashMap<>();
      clientInfoConnectionMap.put(clientInfo1, 10);
      clientInfoConnectionMap.put(clientInfo2, 20);
      when(server1.countConnectedClientsByClientInfo()).thenReturn(clientInfoConnectionMap);
      when(server2.countConnectedClientsByClientInfo()).thenReturn(clientInfoConnectionMap);

      clientMetrics.updateConnectedClients();

      when(server1.countConnectedClientsByClientInfo()).thenReturn(Collections.emptyMap());
      when(server2.countConnectedClientsByClientInfo()).thenReturn(Collections.emptyMap());

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
