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
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelOutboundInvoker;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.Version;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import io.stargate.auth.AuthenticationService;
import io.stargate.cql.impl.CqlImpl;
import io.stargate.db.AuthenticatedUser;
import io.stargate.db.EventListener;
import io.stargate.db.EventListenerWithChannelFilter;
import io.stargate.db.Persistence;
import java.io.IOException;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.net.ResourceLimits;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.stargate.locator.InetAddressAndPort;
import org.apache.cassandra.stargate.metrics.ConnectionMetrics;
import org.apache.cassandra.stargate.transport.ProtocolVersion;
import org.apache.cassandra.stargate.transport.internal.frame.checksum.ChecksummingTransformers;
import org.apache.cassandra.stargate.transport.internal.messages.AuthChallenge;
import org.apache.cassandra.stargate.transport.internal.messages.AuthResponse;
import org.apache.cassandra.stargate.transport.internal.messages.AuthSuccess;
import org.apache.cassandra.stargate.transport.internal.messages.AuthenticateMessage;
import org.apache.cassandra.stargate.transport.internal.messages.BatchMessage;
import org.apache.cassandra.stargate.transport.internal.messages.ErrorMessage;
import org.apache.cassandra.stargate.transport.internal.messages.EventMessage;
import org.apache.cassandra.stargate.transport.internal.messages.ExecuteMessage;
import org.apache.cassandra.stargate.transport.internal.messages.OptionsMessage;
import org.apache.cassandra.stargate.transport.internal.messages.PrepareMessage;
import org.apache.cassandra.stargate.transport.internal.messages.QueryMessage;
import org.apache.cassandra.stargate.transport.internal.messages.ReadyMessage;
import org.apache.cassandra.stargate.transport.internal.messages.RegisterMessage;
import org.apache.cassandra.stargate.transport.internal.messages.ResultMessage;
import org.apache.cassandra.stargate.transport.internal.messages.StartupMessage;
import org.apache.cassandra.stargate.transport.internal.messages.SupportedMessage;
import org.apache.cassandra.stargate.transport.internal.messages.UnsupportedMessageCodec;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CqlServer implements CassandraDaemon.Server {
  static {
    InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
  }

  private static final Logger logger = LoggerFactory.getLogger(CqlServer.class);
  private static final boolean useEpoll = CqlImpl.useEpoll();

  final TransportDescriptor transportDescriptor;
  final PersistenceConnectionFactory persistenceConnectionFactory;
  final ConnectionTracker connectionTracker = new ConnectionTracker();
  // global inflight payload across all channels across all endpoints
  private final ResourceLimits.Concurrent globalRequestPayloadInFlight;
  private final Map<Message.Type, Message.Codec<?>> messageCodecs;

  private final InetSocketAddress socket;
  final Persistence persistence;
  final AuthenticationService authentication;
  public boolean useSSL = false;
  private final AtomicBoolean isRunning = new AtomicBoolean(false);

  private final EventLoopGroup workerGroup;

  private CqlServer(Builder builder) {
    this.transportDescriptor = builder.transportDescriptor;
    this.persistenceConnectionFactory =
        transportDescriptor.isInternal()
            ? new PersistenceConnectionFactory(builder.persistence, builder.authentication)
            : null;
    this.globalRequestPayloadInFlight =
        new ResourceLimits.Concurrent(
            transportDescriptor.getNativeTransportMaxConcurrentRequestsInBytes());
    this.messageCodecs = buildMessageCodecs(transportDescriptor);
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
    this.persistence.registerEventListener(new EventNotifier(this));
  }

  @Override
  public void stop() {
    if (isRunning.compareAndSet(true, false)) close();
  }

  @Override
  public boolean isRunning() {
    return isRunning.get();
  }

  @Override
  public synchronized void start() {
    if (isRunning()) return;

    // Configure the server.
    ServerBootstrap bootstrap =
        new ServerBootstrap()
            .channel(useEpoll ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
            .childOption(ChannelOption.TCP_NODELAY, true)
            .childOption(ChannelOption.SO_LINGER, 0)
            .childOption(ChannelOption.SO_KEEPALIVE, transportDescriptor.getRpcKeepAlive())
            .childOption(ChannelOption.ALLOCATOR, CBUtil.allocator)
            .childOption(
                ChannelOption.WRITE_BUFFER_WATER_MARK,
                new WriteBufferWaterMark(8 * 1024, 32 * 1024));
    if (workerGroup != null) bootstrap = bootstrap.group(workerGroup);

    if (this.useSSL) {
      final EncryptionOptions clientEnc = transportDescriptor.getNativeProtocolEncryptionOptions();

      if (clientEnc.isOptional()) {
        logger.info("Enabling optionally encrypted CQL connections between client and server");
        bootstrap.childHandler(new OptionalSecureInitializer(this, clientEnc));
      } else {
        logger.info("Enabling encrypted CQL connections between client and server");
        bootstrap.childHandler(new SecureInitializer(this, clientEnc));
      }
    } else {
      bootstrap.childHandler(new Initializer(this));
    }

    // Bind and start to accept incoming connections.
    logger.info("Using Netty Version: {}", Version.identify().entrySet());
    logger.info(
        "Starting listening for CQL clients on {} ({})...",
        socket,
        this.useSSL ? "encrypted" : "unencrypted");

    ChannelFuture bindFuture = bootstrap.bind(socket);
    if (!bindFuture.awaitUninterruptibly().isSuccess())
      throw new IllegalStateException(
          String.format(
              "Failed to bind port %d on %s.",
              socket.getPort(), socket.getAddress().getHostAddress()),
          bindFuture.cause());

    connectionTracker.allChannels.add(bindFuture.channel());
    isRunning.set(true);
  }

  private Connection newConnection(Channel channel, ProxyInfo proxyInfo, ProtocolVersion version) {
    return transportDescriptor.isInternal()
        ? new InternalServerConnection(channel, version, this)
        : new ExternalServerConnection(channel, socket.getPort(), proxyInfo, version, this);
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
      if (conn instanceof ExternalServerConnection)
        result.add(new ConnectedClient((ExternalServerConnection) conn));
    }
    return result;
  }

  public List<ClientStat> recentClientStats() {
    return connectionTracker.protocolVersionTracker.getAll();
  }

  @Override
  public void clearConnectionHistory() {
    connectionTracker.protocolVersionTracker.clear();
  }

  private Map<Message.Type, Message.Codec<?>> buildMessageCodecs(
      TransportDescriptor transportDescriptor) {

    Map<Message.Type, Message.Codec<?>> codecs = new EnumMap<>(Message.Type.class);
    codecs.put(Message.Type.ERROR, ErrorMessage.codec);
    codecs.put(
        Message.Type.STARTUP,
        new StartupMessage.Codec(new ChecksummingTransformers(transportDescriptor)));
    codecs.put(Message.Type.READY, ReadyMessage.codec);
    codecs.put(Message.Type.AUTHENTICATE, AuthenticateMessage.codec);
    codecs.put(Message.Type.CREDENTIALS, UnsupportedMessageCodec.instance);
    codecs.put(Message.Type.OPTIONS, OptionsMessage.codec);
    codecs.put(Message.Type.SUPPORTED, SupportedMessage.codec);
    codecs.put(Message.Type.QUERY, QueryMessage.codec);
    codecs.put(Message.Type.RESULT, ResultMessage.codec);
    codecs.put(Message.Type.PREPARE, PrepareMessage.codec);
    codecs.put(Message.Type.EXECUTE, ExecuteMessage.codec);
    codecs.put(Message.Type.REGISTER, RegisterMessage.codec);
    codecs.put(Message.Type.EVENT, EventMessage.codec);
    codecs.put(Message.Type.BATCH, BatchMessage.codec);
    codecs.put(Message.Type.AUTH_CHALLENGE, AuthChallenge.codec);
    codecs.put(Message.Type.AUTH_RESPONSE, AuthResponse.codec);
    codecs.put(Message.Type.AUTH_SUCCESS, AuthSuccess.codec);
    return Collections.unmodifiableMap(codecs);
  }

  private void close() {
    // Close opened connections
    connectionTracker.closeAll();

    logger.info("Stop listening for CQL clients");
  }

  public static class Builder {

    private final TransportDescriptor transportDescriptor;
    private final Persistence persistence;
    private final AuthenticationService authentication;
    private EventLoopGroup workerGroup;
    private boolean useSSL = false;
    private InetAddress hostAddr;
    private int port = -1;
    private InetSocketAddress socket;

    public Builder(
        TransportDescriptor transportDescriptor,
        Persistence persistence,
        AuthenticationService authentication) {
      assert transportDescriptor != null;
      this.transportDescriptor = transportDescriptor;
      assert persistence != null;
      this.persistence = persistence;
      this.authentication = authentication;
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
      groups
          .get(event.type)
          .writeAndFlush(
              new EventMessage(event),
              channel -> {
                if (channel == null || event.headerFilter == null) return true;

                ProxyInfo proxyInfo = channel.attr(ProxyInfo.attributeKey).get();
                Map<String, String> headers =
                    proxyInfo != null ? proxyInfo.toHeaders() : Collections.emptyMap();

                return event.headerFilter.test(headers);
              });
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
        if (connection instanceof ExternalServerConnection) {
          ExternalServerConnection conn = (ExternalServerConnection) connection;
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

  public static class EndpointPayloadTracker {
    // inflight payload per endpoint across corresponding channels
    private static final ConcurrentMap<InetAddress, EndpointPayloadTracker>
        requestPayloadInFlightPerEndpoint = new ConcurrentHashMap<>();

    private final AtomicInteger refCount = new AtomicInteger(0);
    private final InetAddress endpoint;
    private final CqlServer server;
    final ResourceLimits.EndpointAndGlobal endpointAndGlobalPayloadsInFlight;

    private EndpointPayloadTracker(InetAddress endpoint, CqlServer server) {
      this.endpoint = endpoint;
      this.server = server;
      this.endpointAndGlobalPayloadsInFlight =
          new ResourceLimits.EndpointAndGlobal(
              new ResourceLimits.Concurrent(
                  server.transportDescriptor.getNativeTransportMaxConcurrentRequestsInBytesPerIp()),
              server.globalRequestPayloadInFlight);
    }

    public static EndpointPayloadTracker get(InetAddress endpoint, CqlServer server) {
      while (true) {
        EndpointPayloadTracker result =
            requestPayloadInFlightPerEndpoint.computeIfAbsent(
                endpoint, e -> new EndpointPayloadTracker(e, server));
        if (result.acquire()) return result;

        requestPayloadInFlightPerEndpoint.remove(endpoint, result);
      }
    }

    private boolean acquire() {
      return 0 < refCount.updateAndGet(i -> i < 0 ? i : i + 1);
    }

    public void release() {
      if (-1 == refCount.updateAndGet(i -> i == 1 ? -1 : i - 1))
        requestPayloadInFlightPerEndpoint.remove(endpoint, this);
    }
  }

  private static class Initializer extends ChannelInitializer<Channel> {
    // Stateless handlers
    private final Message.ProtocolDecoder messageDecoder;
    private final Message.ProtocolEncoder messageEncoder;
    private final Frame.InboundBodyTransformer inboundFrameTransformer =
        new Frame.InboundBodyTransformer();
    private final Frame.OutboundBodyTransformer outboundFrameTransformer =
        new Frame.OutboundBodyTransformer();
    private final Frame.Encoder frameEncoder = new Frame.Encoder();
    private final Message.ExceptionHandler exceptionHandler = new Message.ExceptionHandler();
    private final ConnectionLimitHandler connectionLimitHandler;

    public final Boolean USE_PROXY_PROTOCOL =
        Boolean.parseBoolean(System.getProperty("stargate.use_proxy_protocol", "false"));

    private final CqlServer server;

    public Initializer(CqlServer server) {
      this.server = server;
      this.connectionLimitHandler = new ConnectionLimitHandler(server.transportDescriptor);
      this.messageDecoder = new Message.ProtocolDecoder(server.messageCodecs);
      this.messageEncoder = new Message.ProtocolEncoder(server.messageCodecs);
    }

    @Override
    protected void initChannel(Channel channel) throws Exception {
      ChannelPipeline pipeline = channel.pipeline();

      // Add the ConnectionLimitHandler to the pipeline if configured to do so.
      if (server.transportDescriptor.getNativeTransportMaxConcurrentConnections() > 0
          || server.transportDescriptor.getNativeTransportMaxConcurrentConnectionsPerIp() > 0) {
        // Add as first to the pipeline so the limit is enforced as first action.
        pipeline.addFirst("connectionLimitHandler", connectionLimitHandler);
      }

      long idleTimeout = server.transportDescriptor.nativeTransportIdleTimeout();
      if (idleTimeout > 0) {
        pipeline.addLast(
            "idleStateHandler",
            new IdleStateHandler(false, 0, 0, idleTimeout, TimeUnit.MILLISECONDS) {
              @Override
              protected void channelIdle(ChannelHandlerContext ctx, IdleStateEvent evt) {
                logger.info(
                    "Closing client connection {} after timeout of {}ms",
                    channel.remoteAddress(),
                    idleTimeout);
                ctx.close();
              }
            });
      }

      if (USE_PROXY_PROTOCOL && !server.transportDescriptor.isInternal()) {
        pipeline.addLast("proxyProtocol", new HAProxyProtocolDetectingDecoder());
      }

      // pipeline.addLast("debug", new LoggingHandler());

      pipeline.addLast(
          "frameDecoder", new Frame.Decoder(server::newConnection, server.transportDescriptor));
      pipeline.addLast("frameEncoder", frameEncoder);

      pipeline.addLast("inboundFrameTransformer", inboundFrameTransformer);
      pipeline.addLast("outboundFrameTransformer", outboundFrameTransformer);

      pipeline.addLast("messageDecoder", messageDecoder);
      pipeline.addLast("messageEncoder", messageEncoder);

      pipeline.addLast(
          "executor",
          new Message.Dispatcher(
              server.transportDescriptor.useNativeTransportLegacyFlusher(),
              EndpointPayloadTracker.get(
                  ((InetSocketAddress) channel.remoteAddress()).getAddress(), server)));

      // The exceptionHandler will take care of handling exceptionCaught(...) events while still
      // running
      // on the same EventLoop as all previous added handlers in the pipeline. This is important as
      // the used
      // eventExecutorGroup may not enforce strict ordering for channel events.
      // As the exceptionHandler runs in the EventLoop as the previous handlers we are sure all
      // exceptions are
      // correctly handled before the handler itself is removed.
      // See https://issues.apache.org/jira/browse/CASSANDRA-13649
      pipeline.addLast("exceptionHandler", exceptionHandler);
    }
  }

  protected abstract static class AbstractSecureIntializer extends Initializer {
    private final EncryptionOptions encryptionOptions;

    protected AbstractSecureIntializer(CqlServer server, EncryptionOptions encryptionOptions) {
      super(server);
      this.encryptionOptions = encryptionOptions;
    }

    protected final SslHandler createSslHandler(ByteBufAllocator allocator) throws IOException {
      SslContext sslContext =
          SSLFactory.getOrCreateSslContext(
              encryptionOptions,
              encryptionOptions.require_client_auth,
              SSLFactory.SocketType.SERVER);
      return sslContext.newHandler(allocator);
    }
  }

  private static class OptionalSecureInitializer extends AbstractSecureIntializer {
    public OptionalSecureInitializer(CqlServer server, EncryptionOptions encryptionOptions) {
      super(server, encryptionOptions);
    }

    @Override
    protected void initChannel(final Channel channel) throws Exception {
      super.initChannel(channel);
      channel
          .pipeline()
          .addFirst(
              "sslDetectionHandler",
              new ByteToMessageDecoder() {
                @Override
                protected void decode(
                    ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list)
                    throws Exception {
                  if (byteBuf.readableBytes() < 5) {
                    // To detect if SSL must be used we need to have at least 5 bytes, so return
                    // here and try again
                    // once more bytes a ready.
                    return;
                  }
                  if (SslHandler.isEncrypted(byteBuf)) {
                    // Connection uses SSL/TLS, replace the detection handler with a SslHandler and
                    // so use
                    // encryption.
                    SslHandler sslHandler = createSslHandler(channel.alloc());
                    channelHandlerContext.pipeline().replace(this, "ssl", sslHandler);
                  } else {
                    // Connection use no TLS/SSL encryption, just remove the detection handler and
                    // continue without
                    // SslHandler in the pipeline.
                    channelHandlerContext.pipeline().remove(this);
                  }
                }
              });
    }
  }

  private static class SecureInitializer extends AbstractSecureIntializer {
    public SecureInitializer(CqlServer server, EncryptionOptions encryptionOptions) {
      super(server, encryptionOptions);
    }

    @Override
    protected void initChannel(Channel channel) throws Exception {
      SslHandler sslHandler = createSslHandler(channel.alloc());
      super.initChannel(channel);
      channel.pipeline().addFirst("ssl", sslHandler);
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
      if (!areEqual(endpoint, FBUtilities.getBroadcastAddressAndPort())
          && event.nodeAddress().equals(FBUtilities.getJustBroadcastNativeAddress())) return;

      send(event);
    }

    private boolean areEqual(
        InetAddressAndPort endpoint1, org.apache.cassandra.locator.InetAddressAndPort endpoint2) {
      return endpoint1 != null
          && endpoint2 != null
          && endpoint1.port == endpoint2.port
          && endpoint1.address.equals(endpoint2.address);
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
