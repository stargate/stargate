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
import org.apache.cassandra.net.AsyncChannelPromise;
import org.apache.cassandra.stargate.transport.ProtocolException;
import org.apache.cassandra.stargate.transport.ProtocolVersion;
import org.apache.cassandra.stargate.transport.ServerError;
import org.apache.cassandra.stargate.transport.internal.messages.ErrorMessage;
import org.apache.cassandra.stargate.transport.internal.messages.StartupMessage;
import org.apache.cassandra.stargate.transport.internal.messages.SupportedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Added to the Netty pipeline whenever a new Channel is initialized. This handler only processes
 * the messages which constitute the initial handshake between client and server, namely OPTIONS and
 * STARTUP. After receiving a STARTUP message, the pipeline is reconfigured according to the
 * protocol version which was negotiated. That reconfiguration should include removing this handler
 * from the pipeline.
 */
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
    Envelope inbound = decoder.decode(buffer);
    if (inbound == null) return;

    try {
      Envelope outbound;
      switch (inbound.header.type) {
        case OPTIONS:
          logger.trace("OPTIONS received {}", inbound.header.version);
          List<String> compressions = new ArrayList<>();
          if (Compressor.SnappyCompressor.instance != null) compressions.add("snappy");
          // LZ4 is always available since worst case scenario it default to a pure JAVA implem.
          compressions.add("lz4");
          final Map<String, List<String>> optionsFromBacked = persistence.cqlSupportedOptions();
          Map<String, List<String>> supportedOptions = new HashMap<>();
          supportedOptions.putAll(optionsFromBacked);
          assert supportedOptions.containsKey(StartupMessage.CQL_VERSION);
          supportedOptions.put(StartupMessage.COMPRESSION, compressions);
          supportedOptions.put(
              StartupMessage.PROTOCOL_VERSIONS, ProtocolVersion.supportedVersions());
          SupportedMessage supported = new SupportedMessage(supportedOptions);
          supported.setStreamId(inbound.header.streamId);
          outbound = supported.encode(inbound.header.version);
          ctx.writeAndFlush(outbound);
          break;

        case STARTUP:
          Attribute<Connection> attrConn = ctx.channel().attr(Connection.attributeKey);
          Connection connection = attrConn.get();
          // Cassandra {4.0.10) Added proxy to newConnection
          ProxyInfo proxy = ctx.channel().attr(ProxyInfo.attributeKey).get();
          if (connection == null) {
            connection = factory.newConnection(ctx.channel(), proxy, inbound.header.version);
            attrConn.set(connection);
          }
          assert connection instanceof ServerConnection;

          StartupMessage startup =
              (StartupMessage) Message.Decoder.decodeMessage(ctx.channel(), inbound);
          InetAddress remoteAddress =
              ((InetSocketAddress) ctx.channel().remoteAddress()).getAddress();
          final ClientResourceLimits.Allocator allocator =
              ClientResourceLimits.getAllocatorForEndpoint(remoteAddress);

          ChannelPromise promise;
          if (inbound.header.version.isGreaterOrEqualTo(ProtocolVersion.V5)) {
            // in this case we need to defer configuring the pipeline until after the response
            // has been sent, as the frame encoding specified in v5 should not be applied to
            // the STARTUP response.
            allocator.allocate(inbound.header.bodySizeInBytes);
            promise =
                AsyncChannelPromise.withListener(
                    ctx,
                    future -> {
                      if (future.isSuccess()) {
                        logger.trace(
                            "Response to STARTUP sent, configuring pipeline for {}",
                            inbound.header.version);
                        configurator.configureModernPipeline(
                            ctx, allocator, inbound.header.version, startup.options);
                        allocator.release(inbound.header.bodySizeInBytes);
                      } else {
                        Throwable cause = future.cause();
                        if (null == cause)
                          cause = new ServerError("Unexpected error establishing connection");
                        logger.warn(
                            "Writing response to STARTUP failed, unable to configure pipeline",
                            cause);
                        ErrorMessage error = ErrorMessage.fromException(cause);
                        Envelope response = error.encode(inbound.header.version);
                        ChannelPromise closeChannel =
                            AsyncChannelPromise.withListener(ctx, f -> ctx.close());
                        ctx.writeAndFlush(response, closeChannel);
                        if (ctx.channel().isOpen()) ctx.channel().close();
                      }
                    });
          } else {
            // no need to configure the pipeline asynchronously in this case
            // the capacity obtained from allocator for the STARTUP message
            // is released when flushed by the legacy dispatcher/flusher so
            // there's no need to explicitly release that here either.
            configurator.configureLegacyPipeline(ctx, allocator);
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
