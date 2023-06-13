//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.cassandra.stargate.transport.internal;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.VoidChannelPromise;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.Attribute;
import io.stargate.db.Persistence;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.net.AsyncChannelPromise;
import org.apache.cassandra.stargate.transport.ProtocolException;
import org.apache.cassandra.stargate.transport.ProtocolVersion;
import org.apache.cassandra.stargate.transport.ServerError;
import org.apache.cassandra.stargate.transport.internal.Compressor.SnappyCompressor;
import org.apache.cassandra.stargate.transport.internal.Message.Decoder;
import org.apache.cassandra.stargate.transport.internal.messages.ErrorMessage;
import org.apache.cassandra.stargate.transport.internal.messages.StartupMessage;
import org.apache.cassandra.stargate.transport.internal.messages.SupportedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InitialConnectionHandler extends ByteToMessageDecoder {
  private static final Logger logger = LoggerFactory.getLogger(InitialConnectionHandler.class);
  final Envelope.Decoder decoder;
  final Connection.Factory factory;
  final PipelineConfigurator configurator;

  // Cassandra {4.0.10} Added persistence as needed in stargate
  private static Persistence persistence;

  InitialConnectionHandler(
      Envelope.Decoder decoder, Connection.Factory factory, PipelineConfigurator configurator) {
    this.decoder = decoder;
    this.factory = factory;
    this.configurator = configurator;
  }

  public static void setPersistence(Persistence inPersistence) {
    persistence = inPersistence;
  }

  protected void decode(ChannelHandlerContext ctx, ByteBuf buffer, List<Object> list)
      throws Exception {
    Envelope inbound = this.decoder.decode(buffer);
    if (inbound != null) {
      try {
        Envelope outbound;
        switch (inbound.header.type) {
          case OPTIONS:
            logger.trace("OPTIONS received {}", inbound.header.version);
            List<String> cqlVersions = new ArrayList();
            cqlVersions.add(QueryProcessor.CQL_VERSION.toString());
            List<String> compressions = new ArrayList();
            if (SnappyCompressor.instance != null) {
              compressions.add("snappy");
            }

            compressions.add("lz4");
            Map<String, List<String>> supportedOptions = new HashMap();
            supportedOptions.put("CQL_VERSION", cqlVersions);
            supportedOptions.put("COMPRESSION", compressions);
            supportedOptions.put("PROTOCOL_VERSIONS", ProtocolVersion.supportedVersions());
            // Cassandra {4.0.10) Patch to add options from persistence
            final Map<String, List<String>> optionsFromBacked = persistence.cqlSupportedOptions();
            supportedOptions.putAll(optionsFromBacked);
            SupportedMessage supported = new SupportedMessage(supportedOptions);
            supported.setStreamId(inbound.header.streamId);
            outbound = supported.encode(inbound.header.version);
            ctx.writeAndFlush(outbound);
            break;
          case STARTUP:
            Attribute<Connection> attrConn = ctx.channel().attr(Connection.attributeKey);
            Connection connection = (Connection) attrConn.get();
            // Cassandra {4.0.10) Added proxy to newConnection
            ProxyInfo proxy = ctx.channel().attr(ProxyInfo.attributeKey).get();
            if (connection == null) {
              connection = this.factory.newConnection(ctx.channel(), proxy, inbound.header.version);
              attrConn.set(connection);
            }

            assert connection instanceof ServerConnection;

            StartupMessage startup = (StartupMessage) Decoder.decodeMessage(ctx.channel(), inbound);
            InetAddress remoteAddress =
                ((InetSocketAddress) ctx.channel().remoteAddress()).getAddress();
            ClientResourceLimits.Allocator allocator =
                ClientResourceLimits.getAllocatorForEndpoint(remoteAddress);
            ChannelPromise promise;
            if (inbound.header.version.isGreaterOrEqualTo(ProtocolVersion.V5)) {
              allocator.allocate(inbound.header.bodySizeInBytes);
              promise =
                  AsyncChannelPromise.withListener(
                      ctx,
                      (future) -> {
                        if (future.isSuccess()) {
                          logger.trace(
                              "Response to STARTUP sent, configuring pipeline for {}",
                              inbound.header.version);
                          this.configurator.configureModernPipeline(
                              ctx, allocator, inbound.header.version, startup.options);
                          allocator.release(inbound.header.bodySizeInBytes);
                        } else {
                          Throwable cause = future.cause();
                          if (null == cause) {
                            cause = new ServerError("Unexpected error establishing connection");
                          }

                          logger.warn(
                              "Writing response to STARTUP failed, unable to configure pipeline",
                              (Throwable) cause);
                          ErrorMessage error = ErrorMessage.fromException((Throwable) cause);
                          Envelope response = error.encode(inbound.header.version);
                          ChannelPromise closeChannel =
                              AsyncChannelPromise.withListener(
                                  ctx,
                                  (f) -> {
                                    ctx.close();
                                  });
                          ctx.writeAndFlush(response, closeChannel);
                          if (ctx.channel().isOpen()) {
                            ctx.channel().close();
                          }
                        }
                      });
            } else {
              this.configurator.configureLegacyPipeline(ctx, allocator);
              promise = new VoidChannelPromise(ctx.channel(), false);
            }

            // Cassandra {4.0.10) Patched similar to current CQL
            CompletableFuture<? extends Message.Response> response =
                Dispatcher.processRequest((ServerConnection) connection, startup);
            response.whenComplete(
                (res, error) -> {
                  Envelope outboundInternal;
                  if (error != null) {
                    if (error instanceof ExecutionException) error = error.getCause();
                    if (error instanceof CompletionException) error = error.getCause();
                    ErrorMessage errorMessage =
                        ErrorMessage.fromException(
                            new ProtocolException(
                                String.format(
                                    "Unexpected message %s, expecting STARTUP or OPTIONS",
                                    inbound.header.type)));
                    outboundInternal = errorMessage.encode(inbound.header.version);
                    ctx.writeAndFlush(outboundInternal);
                  } else {
                    outboundInternal = res.encode(inbound.header.version);
                    ctx.writeAndFlush(outboundInternal, promise);
                    logger.trace("Configured pipeline: {}", ctx.pipeline());
                  }
                });
            break;
          default:
            ErrorMessage error =
                ErrorMessage.fromException(
                    new ProtocolException(
                        String.format(
                            "Unexpected message %s, expecting STARTUP or OPTIONS",
                            inbound.header.type)));
            outbound = error.encode(inbound.header.version);
            ctx.writeAndFlush(outbound);
        }
      } finally {
        inbound.release();
      }
    }
  }
}
