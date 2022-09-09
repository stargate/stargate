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
package org.apache.cassandra.stargate.transport.internal;

import io.micrometer.core.instrument.Tags;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOutboundInvoker;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelMatcher;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import io.stargate.auth.AuthenticationService;
import io.stargate.cql.impl.CqlImpl;
import io.stargate.db.AuthenticatedUser;
import io.stargate.db.EventListener;
import io.stargate.db.EventListenerWithChannelFilter;
import io.stargate.db.Persistence;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.apache.cassandra.net.ResourceLimits;
import org.apache.cassandra.stargate.config.EncryptionOptions;
import org.apache.cassandra.stargate.locator.InetAddressAndPort;
import org.apache.cassandra.stargate.metrics.ConnectionMetrics;
import org.apache.cassandra.stargate.transport.ProtocolVersion;
import org.apache.cassandra.stargate.transport.internal.messages.EventMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CqlServer {
  static {
    InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
  }

  private static final Logger logger = LoggerFactory.getLogger(CqlServer.class);
  private static final boolean useEpoll = CqlImpl.useEpoll();

  private final ConnectionTracker connectionTracker = new ConnectionTracker();

  private final Connection.Factory connectionFactory =
      new Connection.Factory() {
        public Connection newConnection(
            Channel channel, ProxyInfo proxyInfo, ProtocolVersion version) {
          return new ServerConnection(
              channel,
              socket.getPort(),
              proxyInfo,
              version,
              connectionTracker,
              persistence,
              authentication);
        }
      };

  public final InetSocketAddress socket;
  public final Persistence persistence;
  public final AuthenticationService authentication;
  public boolean useSSL = false;
  private final AtomicBoolean isRunning = new AtomicBoolean(false);
  private final PipelineConfigurator pipelineConfigurator;
  private final EventLoopGroup workerGroup;

  private CqlServer(Builder builder) {
    this.persistence = builder.persistence;
    this.authentication = builder.authentication;
    this.socket = builder.getSocket();
    this.useSSL = builder.useSSL;
    if (builder.workerGroup != null) {
      workerGroup = builder.workerGroup;
    } else {
      if (useEpoll) workerGroup = new EpollEventLoopGroup();
      else workerGroup = new NioEventLoopGroup();
    }
    InitialConnectionHandler.setPersistence(persistence);
    pipelineConfigurator =
        builder.pipelineConfigurator != null
            ? builder.pipelineConfigurator
            : new PipelineConfigurator(
                useEpoll,
                TransportDescriptor.getRpcKeepAlive(),
                TransportDescriptor.useNativeTransportLegacyFlusher(),
                builder.tlsEncryptionPolicy);
    this.persistence.registerEventListener(new EventNotifier(this));
  }

  public void stop() {
    if (isRunning.compareAndSet(true, false)) close();
  }

  public boolean isRunning() {
    return isRunning.get();
  }

  public synchronized void start() {
    if (isRunning()) return;

    // Configure the server.
    ChannelFuture bindFuture =
        pipelineConfigurator.initializeChannel(workerGroup, socket, connectionFactory);
    if (!bindFuture.awaitUninterruptibly().isSuccess())
      throw new IllegalStateException(
          String.format(
              "Failed to bind port %d on %s.",
              socket.getPort(), socket.getAddress().getHostAddress()),
          bindFuture.cause());

    connectionTracker.allChannels.add(bindFuture.channel());
    isRunning.set(true);
  }

  public int countConnectedClients() {
    return connectionTracker.countConnectedClients();
  }

  public Map<String, Integer> countConnectedClientsByUser() {
    return connectionTracker.countConnectedClientsByUser();
  }

  public Map<Tags, Integer> countConnectedClientsByConnectionTags() {
    return connectionTracker.countConnectedClientsByConnectionTags();
  }

  public List<ConnectedClient> getConnectedClients() {
    List<ConnectedClient> result = new ArrayList<>();
    for (Channel c : connectionTracker.allChannels) {
      Connection conn = c.attr(Connection.attributeKey).get();
      if (conn instanceof ServerConnection)
        result.add(new ConnectedClient((ServerConnection) conn));
    }
    return result;
  }

  public List<ClientStat> recentClientStats() {
    return connectionTracker.protocolVersionTracker.getAll();
  }

  public void clearConnectionHistory() {
    connectionTracker.protocolVersionTracker.clear();
  }

  private void close() {
    // Close opened connections
    connectionTracker.closeAll();

    logger.info("Stop listening for CQL clients");
  }

  public static class Builder {

    private final Persistence persistence;
    private final AuthenticationService authentication;
    private EncryptionOptions.TlsEncryptionPolicy tlsEncryptionPolicy =
        EncryptionOptions.TlsEncryptionPolicy.UNENCRYPTED;
    private EventLoopGroup workerGroup;
    private boolean useSSL = false;
    private InetAddress hostAddr;
    private int port = -1;
    private InetSocketAddress socket;
    private PipelineConfigurator pipelineConfigurator;

    public Builder(Persistence persistence, AuthenticationService authentication) {
      assert persistence != null;
      this.persistence = persistence;
      this.authentication = authentication;
    }

    public Builder withTlsEncryptionPolicy(
        EncryptionOptions.TlsEncryptionPolicy tlsEncryptionPolicy) {
      this.tlsEncryptionPolicy = tlsEncryptionPolicy;
      return this;
    }

    public Builder withSSL(boolean useSSL) {
      this.useSSL = useSSL;
      return this;
    }

    public Builder withEventLoopGroup(EventLoopGroup eventLoopGroup) {
      this.workerGroup = eventLoopGroup;
      return this;
    }

    public Builder withHost(InetAddress host) {
      this.hostAddr = host;
      this.socket = null;
      return this;
    }

    public Builder withPort(int port) {
      this.port = port;
      this.socket = null;
      return this;
    }

    public Builder withPipelineConfigurator(PipelineConfigurator configurator) {
      this.pipelineConfigurator = configurator;
      return this;
    }

    public CqlServer build() {
      return new CqlServer(this);
    }

    private InetSocketAddress getSocket() {
      if (this.socket != null) return this.socket;
      else {
        if (this.port == -1) throw new IllegalStateException("Missing port number");
        if (this.hostAddr != null) this.socket = new InetSocketAddress(this.hostAddr, this.port);
        else throw new IllegalStateException("Missing host");
        return this.socket;
      }
    }
  }

  public static class ConnectionTracker implements Connection.Tracker {
    // TODO: should we be using the GlobalEventExecutor or defining our own?
    private static final ChannelMatcher PRE_V5_CHANNEL =
        channel ->
            channel
                .attr(Connection.attributeKey)
                .get()
                .getVersion()
                .isSmallerThan(ProtocolVersion.V5);
    public final ChannelGroup allChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    private final EnumMap<Event.Type, ChannelGroup> groups = new EnumMap<>(Event.Type.class);
    private final ProtocolVersionTracker protocolVersionTracker = new ProtocolVersionTracker();

    public ConnectionTracker() {
      for (Event.Type type : Event.Type.values())
        groups.put(type, new DefaultChannelGroup(type.toString(), GlobalEventExecutor.INSTANCE));
    }

    @Override
    public void addConnection(Channel ch, Connection connection) {
      allChannels.add(ch);

      if (ch.remoteAddress() instanceof InetSocketAddress)
        protocolVersionTracker.addConnection(
            ((InetSocketAddress) ch.remoteAddress()).getAddress(), connection.getVersion());
    }

    public void register(Event.Type type, Channel ch) {
      groups.get(type).add(ch);
    }

    public void send(Event event) {
      groups.get(event.type).writeAndFlush(new EventMessage(event), PRE_V5_CHANNEL);
    }

    void closeAll() {
      allChannels.close().awaitUninterruptibly();
    }

    /**
     * Close any channels that match the header filter predicate.
     *
     * @param headerFilter a predicate used to match affected clients.
     */
    void closeFilter(Predicate<Map<String, String>> headerFilter) {
      allChannels.stream()
          .filter(
              channel -> {
                ProxyInfo proxyInfo = channel.attr(ProxyInfo.attributeKey).get();
                Map<String, String> headers =
                    proxyInfo != null ? proxyInfo.toHeaders() : Collections.emptyMap();
                return headerFilter.test(headers);
              })
          .forEach(ChannelOutboundInvoker::close);
    }

    int countConnectedClients() {
      /*
        - When server is running: allChannels contains all clients' connections (channels)
          plus one additional channel used for the server's own bootstrap.
         - When server is stopped: the size is 0
      */
      return allChannels.size() != 0 ? allChannels.size() - 1 : 0;
    }

    Map<String, Integer> countConnectedClientsByUser() {
      Map<String, Integer> result = new HashMap<>();
      for (Channel c : allChannels) {
        Connection connection = c.attr(Connection.attributeKey).get();
        if (connection instanceof ServerConnection) {
          ServerConnection conn = (ServerConnection) connection;
          Optional<AuthenticatedUser> user = conn.persistenceConnection().loggedUser();
          String name = user.map(AuthenticatedUser::name).orElse("unknown");
          result.put(name, result.getOrDefault(name, 0) + 1);
        }
      }
      return result;
    }

    Map<Tags, Integer> countConnectedClientsByConnectionTags() {
      Map<Tags, Integer> result = new HashMap<>();
      for (Channel c : allChannels) {
        Connection connection = c.attr(Connection.attributeKey).get();
        if (null != connection) {
          ConnectionMetrics connectionMetrics = connection.getConnectionMetrics();
          Tags tags = connectionMetrics.getTags();
          result.compute(tags, (t, v) -> v == null ? 1 : v + 1);
        }
      }
      return result;
    }
  }

  // global inflight payload across all channels across all endpoints
  private static final ResourceLimits.Concurrent globalRequestPayloadInFlight =
      new ResourceLimits.Concurrent(
          TransportDescriptor.getNativeTransportMaxConcurrentRequestsInBytes());

  public static class EndpointPayloadTracker {
    // inflight payload per endpoint across corresponding channels
    private static final ConcurrentMap<InetAddress, EndpointPayloadTracker>
        requestPayloadInFlightPerEndpoint = new ConcurrentHashMap<>();

    private final AtomicInteger refCount = new AtomicInteger(0);
    private final InetAddress endpoint;

    final ResourceLimits.EndpointAndGlobal endpointAndGlobalPayloadsInFlight =
        new ResourceLimits.EndpointAndGlobal(
            new ResourceLimits.Concurrent(
                TransportDescriptor.getNativeTransportMaxConcurrentRequestsInBytesPerIp()),
            globalRequestPayloadInFlight);

    private EndpointPayloadTracker(InetAddress endpoint) {
      this.endpoint = endpoint;
    }

    public static EndpointPayloadTracker get(InetAddress endpoint) {
      while (true) {
        EndpointPayloadTracker result =
            requestPayloadInFlightPerEndpoint.computeIfAbsent(
                endpoint, EndpointPayloadTracker::new);
        if (result.acquire()) return result;

        requestPayloadInFlightPerEndpoint.remove(endpoint, result);
      }
    }

    public static long getGlobalLimit() {
      return TransportDescriptor.getNativeTransportMaxConcurrentRequestsInBytes();
    }

    public static void setGlobalLimit(long newLimit) {
      TransportDescriptor.setNativeTransportMaxConcurrentRequestsInBytes(newLimit);
      long existingLimit =
          globalRequestPayloadInFlight.setLimit(
              TransportDescriptor.getNativeTransportMaxConcurrentRequestsInBytes());

      logger.info(
          "Changed native_max_transport_requests_in_bytes from {} to {}", existingLimit, newLimit);
    }

    public static long getEndpointLimit() {
      return TransportDescriptor.getNativeTransportMaxConcurrentRequestsInBytesPerIp();
    }

    public static void setEndpointLimit(long newLimit) {
      long existingLimit =
          TransportDescriptor.getNativeTransportMaxConcurrentRequestsInBytesPerIp();
      TransportDescriptor.setNativeTransportMaxConcurrentRequestsInBytesPerIp(
          newLimit); // ensure new trackers get the new limit
      for (EndpointPayloadTracker tracker : requestPayloadInFlightPerEndpoint.values())
        existingLimit = tracker.endpointAndGlobalPayloadsInFlight.endpoint().setLimit(newLimit);

      logger.info(
          "Changed native_max_transport_requests_in_bytes_per_ip from {} to {}",
          existingLimit,
          newLimit);
    }

    private boolean acquire() {
      return 0 < refCount.updateAndGet(i -> i < 0 ? i : i + 1);
    }

    public void release() {
      if (-1 == refCount.updateAndGet(i -> i == 1 ? -1 : i - 1))
        requestPayloadInFlightPerEndpoint.remove(endpoint, this);
    }
  }

  private static class LatestEvent {
    public final Event.StatusChange.Status status;
    public final Event.TopologyChange.Change topology;

    private LatestEvent(Event.StatusChange.Status status, Event.TopologyChange.Change topology) {
      this.status = status;
      this.topology = topology;
    }

    @Override
    public String toString() {
      return String.format("Status %s, Topology %s", status, topology);
    }

    public static LatestEvent forStatusChange(Event.StatusChange.Status status, LatestEvent prev) {
      return new LatestEvent(status, prev == null ? null : prev.topology);
    }

    public static LatestEvent forTopologyChange(
        Event.TopologyChange.Change change, LatestEvent prev) {
      return new LatestEvent(prev == null ? null : prev.status, change);
    }
  }

  private static class EventNotifier implements EventListenerWithChannelFilter {
    private final CqlServer server;

    // We keep track of the latest status change events we have sent to avoid sending duplicates
    // since StorageService may send duplicate notifications (CASSANDRA-7816, CASSANDRA-8236,
    // CASSANDRA-9156)
    private final Map<InetAddressAndPort, LatestEvent> latestEvents = new ConcurrentHashMap<>();

    private EventNotifier(CqlServer server) {
      this.server = server;
    }

    private void send(InetAddressAndPort endpoint, Event.NodeEvent event) {
      if (logger.isTraceEnabled())
        logger.trace(
            "Sending event for endpoint {}, rpc address {}", endpoint, event.nodeAddress());

      // If the endpoint is not the local node, extract the node address
      // and if it is the same as our own RPC broadcast address (which defaults to the rcp address)
      // then don't send the notification. This covers the case of rpc_address set to "localhost",
      // which is not useful to any driver and in fact may cauase serious problems to some drivers,
      // see CASSANDRA-10052
      if (event.nodeAddress().equals(TransportDescriptor.getRpcAddress())) return;

      send(event);
    }

    private void send(Event event) {
      server.connectionTracker.send(event);
    }

    @Override
    public void onJoinCluster(
        InetAddress endpoint, int port, Predicate<Map<String, String>> headerFilter) {
      InetAddressAndPort endpointWithPort = addPort(endpoint, port);
      onTopologyChange(
          endpointWithPort, Event.TopologyChange.newNode(endpointWithPort, headerFilter));
    }

    @Override
    public void onLeaveCluster(
        InetAddress endpoint, int port, Predicate<Map<String, String>> headerFilter) {
      InetAddressAndPort endpointWithPort = addPort(endpoint, port);
      onTopologyChange(
          endpointWithPort, Event.TopologyChange.removedNode(endpointWithPort, headerFilter));
    }

    @Override
    public void onMove(
        InetAddress endpoint, int port, Predicate<Map<String, String>> headerFilter) {
      InetAddressAndPort endpointWithPort = addPort(endpoint, port);
      onTopologyChange(
          endpointWithPort, Event.TopologyChange.movedNode(endpointWithPort, headerFilter));
    }

    @Override
    public void onDown(
        InetAddress endpoint, int port, Predicate<Map<String, String>> headerFilter) {
      InetAddressAndPort endpointWithPort = addPort(endpoint, port);
      onStatusChange(endpointWithPort, Event.StatusChange.nodeDown(endpointWithPort, headerFilter));
    }

    @Override
    public void onUp(InetAddress endpoint, int port, Predicate<Map<String, String>> headerFilter) {
      InetAddressAndPort endpointWithPort = addPort(endpoint, port);
      onStatusChange(endpointWithPort, Event.StatusChange.nodeUp(endpointWithPort, headerFilter));
    }

    @Override
    public void onClose(Predicate<Map<String, String>> headerFilter) {
      if (headerFilter != null) {
        server.connectionTracker.closeFilter(headerFilter);
      }
    }

    private InetAddressAndPort addPort(InetAddress endpoint, int port) {
      return InetAddressAndPort.getByAddressOverrideDefaults(endpoint, getPortOrDefault(port));
    }

    /**
     * If no explicit port is specified then use the server socket's listening port. An explicit
     * port is provided for Cassandra 4.0 peers and when using proxy protocol.
     *
     * @param port An explicit port to use or {@link EventListener#NO_PORT}
     * @return The server socket's listening port when {@link EventListener#NO_PORT} is provided;
     *     otherwise, the original port value is returned.
     */
    private int getPortOrDefault(int port) {
      return port == NO_PORT ? server.socket.getPort() : port;
    }

    private void onTopologyChange(InetAddressAndPort endpoint, Event.TopologyChange event) {
      if (logger.isTraceEnabled())
        logger.trace("Topology changed event : {}, {}", endpoint, event.change);

      if (event.headerFilter != null) {
        send(endpoint, event);
      } else {
        LatestEvent prev = latestEvents.get(endpoint);
        if (prev == null || prev.topology != event.change) {
          LatestEvent ret =
              latestEvents.put(endpoint, LatestEvent.forTopologyChange(event.change, prev));
          if (ret == prev) send(endpoint, event);
        }
      }
    }

    private void onStatusChange(InetAddressAndPort endpoint, Event.StatusChange event) {
      if (logger.isTraceEnabled())
        logger.trace("Status changed event : {}, {}", endpoint, event.status);

      if (event.headerFilter != null) {
        send(endpoint, event);
      } else {
        LatestEvent prev = latestEvents.get(endpoint);
        if (prev == null || prev.status != event.status) {
          LatestEvent ret =
              latestEvents.put(endpoint, LatestEvent.forStatusChange(event.status, null));
          if (ret == prev) send(endpoint, event);
        }
      }
    }

    @Override
    public void onCreateKeyspace(String ksName, Predicate<Map<String, String>> headerFilter) {
      send(new Event.SchemaChange(Event.SchemaChange.Change.CREATED, ksName, headerFilter));
    }

    @Override
    public void onCreateTable(
        String ksName, String cfName, Predicate<Map<String, String>> headerFilter) {
      send(
          new Event.SchemaChange(
              Event.SchemaChange.Change.CREATED,
              Event.SchemaChange.Target.TABLE,
              ksName,
              cfName,
              headerFilter));
    }

    @Override
    public void onCreateType(
        String ksName, String typeName, Predicate<Map<String, String>> headerFilter) {
      send(
          new Event.SchemaChange(
              Event.SchemaChange.Change.CREATED,
              Event.SchemaChange.Target.TYPE,
              ksName,
              typeName,
              headerFilter));
    }

    @Override
    public void onCreateFunction(
        String ksName,
        String functionName,
        List<String> argTypes,
        Predicate<Map<String, String>> headerFilter) {
      send(
          new Event.SchemaChange(
              Event.SchemaChange.Change.CREATED,
              Event.SchemaChange.Target.FUNCTION,
              ksName,
              functionName,
              argTypes,
              headerFilter));
    }

    @Override
    public void onCreateAggregate(
        String ksName,
        String aggregateName,
        List<String> argTypes,
        Predicate<Map<String, String>> headerFilter) {
      send(
          new Event.SchemaChange(
              Event.SchemaChange.Change.CREATED,
              Event.SchemaChange.Target.AGGREGATE,
              ksName,
              aggregateName,
              argTypes,
              headerFilter));
    }

    @Override
    public void onAlterKeyspace(String ksName, Predicate<Map<String, String>> headerFilter) {
      send(new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, ksName, headerFilter));
    }

    @Override
    public void onAlterTable(
        String ksName, String cfName, Predicate<Map<String, String>> headerFilter) {
      send(
          new Event.SchemaChange(
              Event.SchemaChange.Change.UPDATED,
              Event.SchemaChange.Target.TABLE,
              ksName,
              cfName,
              headerFilter));
    }

    @Override
    public void onAlterType(
        String ksName, String typeName, Predicate<Map<String, String>> headerFilter) {
      send(
          new Event.SchemaChange(
              Event.SchemaChange.Change.UPDATED,
              Event.SchemaChange.Target.TYPE,
              ksName,
              typeName,
              headerFilter));
    }

    @Override
    public void onAlterFunction(
        String ksName,
        String functionName,
        List<String> argTypes,
        Predicate<Map<String, String>> headerFilter) {
      send(
          new Event.SchemaChange(
              Event.SchemaChange.Change.UPDATED,
              Event.SchemaChange.Target.FUNCTION,
              ksName,
              functionName,
              argTypes,
              headerFilter));
    }

    @Override
    public void onAlterAggregate(
        String ksName,
        String aggregateName,
        List<String> argTypes,
        Predicate<Map<String, String>> headerFilter) {
      send(
          new Event.SchemaChange(
              Event.SchemaChange.Change.UPDATED,
              Event.SchemaChange.Target.AGGREGATE,
              ksName,
              aggregateName,
              argTypes,
              headerFilter));
    }

    @Override
    public void onDropKeyspace(String ksName, Predicate<Map<String, String>> headerFilter) {
      send(new Event.SchemaChange(Event.SchemaChange.Change.DROPPED, ksName, headerFilter));
    }

    @Override
    public void onDropTable(
        String ksName, String cfName, Predicate<Map<String, String>> headerFilter) {
      send(
          new Event.SchemaChange(
              Event.SchemaChange.Change.DROPPED,
              Event.SchemaChange.Target.TABLE,
              ksName,
              cfName,
              headerFilter));
    }

    @Override
    public void onDropType(
        String ksName, String typeName, Predicate<Map<String, String>> headerFilter) {
      send(
          new Event.SchemaChange(
              Event.SchemaChange.Change.DROPPED,
              Event.SchemaChange.Target.TYPE,
              ksName,
              typeName,
              headerFilter));
    }

    @Override
    public void onDropFunction(
        String ksName,
        String functionName,
        List<String> argTypes,
        Predicate<Map<String, String>> headerFilter) {
      send(
          new Event.SchemaChange(
              Event.SchemaChange.Change.DROPPED,
              Event.SchemaChange.Target.FUNCTION,
              ksName,
              functionName,
              argTypes,
              headerFilter));
    }

    @Override
    public void onDropAggregate(
        String ksName,
        String aggregateName,
        List<String> argTypes,
        Predicate<Map<String, String>> headerFilter) {
      send(
          new Event.SchemaChange(
              Event.SchemaChange.Change.DROPPED,
              Event.SchemaChange.Target.AGGREGATE,
              ksName,
              aggregateName,
              argTypes,
              headerFilter));
    }
  }
}
