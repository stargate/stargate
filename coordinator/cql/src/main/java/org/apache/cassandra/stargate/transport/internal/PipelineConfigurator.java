//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.cassandra.stargate.transport.internal;

import com.google.common.base.Strings;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.Version;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.AbstractMessageHandler;
import org.apache.cassandra.net.BufferPoolAllocator;
import org.apache.cassandra.net.FrameDecoder;
import org.apache.cassandra.net.FrameDecoderCrc;
import org.apache.cassandra.net.FrameDecoderLZ4;
import org.apache.cassandra.net.FrameEncoder;
import org.apache.cassandra.net.FrameEncoderCrc;
import org.apache.cassandra.net.FrameEncoderLZ4;
import org.apache.cassandra.net.GlobalBufferPoolAllocator;
import org.apache.cassandra.stargate.config.EncryptionOptions;
import org.apache.cassandra.stargate.security.SSLFactory;
import org.apache.cassandra.stargate.security.SSLFactory.SocketType;
import org.apache.cassandra.stargate.transport.ProtocolException;
import org.apache.cassandra.stargate.transport.ProtocolVersion;
import org.apache.cassandra.transport.Envelope.Compressor;
import org.apache.cassandra.transport.Envelope.Decompressor;
import org.apache.cassandra.transport.Envelope.Encoder;
import org.apache.cassandra.transport.PreV5Handlers.ExceptionHandler;
import org.apache.cassandra.transport.PreV5Handlers.ProtocolDecoder;
import org.apache.cassandra.transport.PreV5Handlers.ProtocolEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineConfigurator {
  private static final Logger logger = LoggerFactory.getLogger(PipelineConfigurator.class);
  private static final boolean DEBUG =
      Boolean.getBoolean("cassandra.unsafe_verbose_debug_client_protocol");
  private static final ConnectionLimitHandler connectionLimitHandler = new ConnectionLimitHandler();
  private static final String CONNECTION_LIMIT_HANDLER = "connectionLimitHandler";
  private static final String IDLE_STATE_HANDLER = "idleStateHandler";
  private static final String INITIAL_HANDLER = "initialHandler";
  private static final String EXCEPTION_HANDLER = "exceptionHandler";
  private static final String DEBUG_HANDLER = "debugHandler";
  private static final String SSL_HANDLER = "ssl";
  private static final String ENVELOPE_DECODER = "envelopeDecoder";
  private static final String ENVELOPE_ENCODER = "envelopeEncoder";
  private static final String MESSAGE_DECOMPRESSOR = "decompressor";
  private static final String MESSAGE_COMPRESSOR = "compressor";
  private static final String MESSAGE_DECODER = "messageDecoder";
  private static final String MESSAGE_ENCODER = "messageEncoder";
  private static final String LEGACY_MESSAGE_PROCESSOR = "legacyCqlProcessor";
  private static final String FRAME_DECODER = "frameDecoder";
  private static final String FRAME_ENCODER = "frameEncoder";
  private static final String MESSAGE_PROCESSOR = "cqlProcessor";
  private final boolean epoll;
  private final boolean keepAlive;
  private final EncryptionOptions.TlsEncryptionPolicy tlsEncryptionPolicy;
  private final Dispatcher dispatcher;

  public PipelineConfigurator(
      boolean epoll,
      boolean keepAlive,
      boolean legacyFlusher,
      EncryptionOptions.TlsEncryptionPolicy encryptionPolicy) {
    this.epoll = epoll;
    this.keepAlive = keepAlive;
    this.tlsEncryptionPolicy = encryptionPolicy;
    this.dispatcher = this.dispatcher(legacyFlusher);
  }

  public ChannelFuture initializeChannel(
      EventLoopGroup workerGroup, InetSocketAddress socket, Connection.Factory connectionFactory) {
    ServerBootstrap bootstrap =
        ((ServerBootstrap)
                (new ServerBootstrap())
                    .channel(
                        this.epoll ? EpollServerSocketChannel.class : NioServerSocketChannel.class))
            .childOption(ChannelOption.TCP_NODELAY, true)
            .childOption(ChannelOption.SO_LINGER, 0)
            .childOption(ChannelOption.SO_KEEPALIVE, this.keepAlive)
            .childOption(ChannelOption.ALLOCATOR, CBUtil.allocator)
            .childOption(
                ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(8192, 32768));
    if (workerGroup != null) {
      bootstrap = bootstrap.group(workerGroup);
    }

    ChannelInitializer<Channel> initializer = this.initializer(connectionFactory);
    bootstrap.childHandler(initializer);
    logger.info("Using Netty Version: {}", Version.identify().entrySet());
    logger.info(
        "Starting listening for CQL clients on {} ({})...",
        socket,
        this.tlsEncryptionPolicy.description());
    return bootstrap.bind(socket);
  }

  protected ChannelInitializer<Channel> initializer(final Connection.Factory connectionFactory) {
    final EncryptionConfig encryptionConfig = this.encryptionConfig();
    return new ChannelInitializer<Channel>() {
      protected void initChannel(Channel channel) throws Exception {
        PipelineConfigurator.this.configureInitialPipeline(channel, connectionFactory);
        encryptionConfig.applyTo(channel);
      }
    };
  }

  protected EncryptionConfig encryptionConfig() {
    EncryptionOptions encryptionOptions = TransportDescriptor.getNativeProtocolEncryptionOptions();
    switch (this.tlsEncryptionPolicy) {
      case UNENCRYPTED:
        return (channel) -> {};
      case OPTIONAL:
        logger.debug("Enabling optionally encrypted CQL connections between client and server");
        return (channel) -> {
          final SslContext sslContext =
              SSLFactory.getOrCreateSslContext(
                  encryptionOptions, encryptionOptions.require_client_auth, SocketType.SERVER);
          channel
              .pipeline()
              .addFirst(
                  "ssl",
                  new ByteToMessageDecoder() {
                    protected void decode(
                        ChannelHandlerContext channelHandlerContext,
                        ByteBuf byteBuf,
                        List<Object> list)
                        throws Exception {
                      if (byteBuf.readableBytes() >= 5) {
                        if (SslHandler.isEncrypted(byteBuf)) {
                          SslHandler sslHandler = sslContext.newHandler(channel.alloc());
                          channelHandlerContext.pipeline().replace("ssl", "ssl", sslHandler);
                        } else {
                          channelHandlerContext.pipeline().remove("ssl");
                        }
                      }
                    }
                  });
        };
      case ENCRYPTED:
        logger.debug("Enabling encrypted CQL connections between client and server");
        return (channel) -> {
          SslContext sslContext =
              SSLFactory.getOrCreateSslContext(
                  encryptionOptions, encryptionOptions.require_client_auth, SocketType.SERVER);
          channel.pipeline().addFirst("ssl", sslContext.newHandler(channel.alloc()));
        };
      default:
        throw new IllegalStateException(
            "Unrecognized TLS encryption policy: " + this.tlsEncryptionPolicy);
    }
  }

  public void configureInitialPipeline(
      final Channel channel, Connection.Factory connectionFactory) {
    ChannelPipeline pipeline = channel.pipeline();
    if (DatabaseDescriptor.getNativeTransportMaxConcurrentConnections() > 0L
        || DatabaseDescriptor.getNativeTransportMaxConcurrentConnectionsPerIp() > 0L) {
      pipeline.addFirst("connectionLimitHandler", connectionLimitHandler);
    }

    final long idleTimeout = DatabaseDescriptor.nativeTransportIdleTimeout();
    if (idleTimeout > 0L) {
      pipeline.addLast(
          "idleStateHandler",
          new IdleStateHandler(false, 0L, 0L, idleTimeout, TimeUnit.MILLISECONDS) {
            protected void channelIdle(ChannelHandlerContext ctx, IdleStateEvent evt) {
              PipelineConfigurator.logger.info(
                  "Closing client connection {} after timeout of {}ms",
                  channel.remoteAddress(),
                  idleTimeout);
              ctx.close();
            }
          });
    }

    if (DEBUG) {
      pipeline.addLast("debugHandler", new LoggingHandler(LogLevel.INFO));
    }

    pipeline.addLast("envelopeEncoder", Encoder.instance);
    pipeline.addLast(
        "initialHandler",
        new InitialConnectionHandler(new Envelope.Decoder(), connectionFactory, this));
    pipeline.addLast("exceptionHandler", ExceptionHandler.instance);
    this.onInitialPipelineReady(pipeline);
  }

  public void configureModernPipeline(
      ChannelHandlerContext ctx,
      ClientResourceLimits.Allocator resourceAllocator,
      ProtocolVersion version,
      Map<String, String> options) {
    BufferPoolAllocator allocator = GlobalBufferPoolAllocator.instance;
    ctx.channel().config().setOption(ChannelOption.ALLOCATOR, allocator);
    String compression = (String) options.get("COMPRESSION");
    FrameDecoder frameDecoder = this.frameDecoder(compression, allocator);
    FrameEncoder frameEncoder = this.frameEncoder(compression);
    FrameEncoder.PayloadAllocator payloadAllocator = frameEncoder.allocator();
    ChannelInboundHandlerAdapter exceptionHandler =
        ExceptionHandlers.postV5Handler(payloadAllocator, version);
    Message.Decoder<Message.Request> messageDecoder = this.messageDecoder();
    Envelope.Decoder envelopeDecoder = new Envelope.Decoder();
    ChannelPipeline pipeline = ctx.channel().pipeline();
    ChannelHandlerContext firstContext = pipeline.firstContext();
    CQLMessageHandler.ErrorHandler errorHandler = firstContext::fireExceptionCaught;
    int queueCapacity = DatabaseDescriptor.getNativeTransportReceiveQueueCapacityInBytes();
    ClientResourceLimits.ResourceProvider resourceProvider =
        this.resourceProvider(resourceAllocator);
    AbstractMessageHandler.OnHandlerClosed onClosed =
        (handler) -> {
          resourceProvider.release();
        };
    boolean throwOnOverload = "1".equals(options.get("THROW_ON_OVERLOAD"));
    CQLMessageHandler.MessageConsumer<Message.Request> messageConsumer = this.messageConsumer();
    CQLMessageHandler<Message.Request> processor =
        new CQLMessageHandler(
            ctx.channel(),
            version,
            frameDecoder,
            envelopeDecoder,
            messageDecoder,
            messageConsumer,
            payloadAllocator,
            queueCapacity,
            resourceProvider,
            onClosed,
            errorHandler,
            throwOnOverload);
    pipeline.remove("envelopeEncoder");
    pipeline.addBefore("initialHandler", "frameDecoder", frameDecoder);
    pipeline.addBefore("initialHandler", "frameEncoder", frameEncoder);
    pipeline.addBefore("initialHandler", "cqlProcessor", processor);
    pipeline.replace("exceptionHandler", "exceptionHandler", exceptionHandler);
    pipeline.remove("initialHandler");
    ctx.channel()
        .attr(Dispatcher.EVENT_DISPATCHER)
        .set(this.dispatcher.eventDispatcher(ctx.channel(), version, payloadAllocator));
    this.onNegotiationComplete(pipeline);
  }

  protected void onInitialPipelineReady(ChannelPipeline pipeline) {}

  protected void onNegotiationComplete(ChannelPipeline pipeline) {}

  protected ClientResourceLimits.ResourceProvider resourceProvider(
      ClientResourceLimits.Allocator allocator) {
    return new ClientResourceLimits.ResourceProvider.Default(allocator);
  }

  protected Dispatcher dispatcher(boolean useLegacyFlusher) {
    return new Dispatcher(useLegacyFlusher);
  }

  protected CQLMessageHandler.MessageConsumer<Message.Request> messageConsumer() {
    Dispatcher var10000 = this.dispatcher;
    return var10000::dispatch;
  }

  protected Message.Decoder<Message.Request> messageDecoder() {
    return Message.requestDecoder();
  }

  protected FrameDecoder frameDecoder(String compression, BufferPoolAllocator allocator) {
    if (null == compression) {
      return FrameDecoderCrc.create(allocator);
    } else if (compression.equalsIgnoreCase("LZ4")) {
      return FrameDecoderLZ4.fast(allocator);
    } else {
      throw new ProtocolException("Unsupported compression type: " + compression);
    }
  }

  protected FrameEncoder frameEncoder(String compression) {
    if (Strings.isNullOrEmpty(compression)) {
      return FrameEncoderCrc.instance;
    } else if (compression.equalsIgnoreCase("LZ4")) {
      return FrameEncoderLZ4.fastInstance;
    } else {
      throw new ProtocolException("Unsupported compression type: " + compression);
    }
  }

  public void configureLegacyPipeline(
      ChannelHandlerContext ctx, ClientResourceLimits.Allocator limits) {
    ChannelPipeline pipeline = ctx.channel().pipeline();
    pipeline.addBefore("envelopeEncoder", "envelopeDecoder", new Envelope.Decoder());
    pipeline.addBefore("initialHandler", "decompressor", Decompressor.instance);
    pipeline.addBefore("initialHandler", "compressor", Compressor.instance);
    pipeline.addBefore("initialHandler", "messageDecoder", ProtocolDecoder.instance);
    pipeline.addBefore("initialHandler", "messageEncoder", ProtocolEncoder.instance);
    pipeline.addBefore(
        "initialHandler",
        "legacyCqlProcessor",
        new PreV5Handlers.LegacyDispatchHandler(this.dispatcher, limits));
    pipeline.remove("initialHandler");
    this.onNegotiationComplete(pipeline);
  }

  interface EncryptionConfig {
    void applyTo(Channel var1) throws Exception;
  }
}
