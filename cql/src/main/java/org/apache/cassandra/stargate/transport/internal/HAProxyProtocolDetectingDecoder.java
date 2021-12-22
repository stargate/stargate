package org.apache.cassandra.stargate.transport.internal;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyMessageDecoder;
import io.netty.util.Attribute;
import java.net.InetSocketAddress;
import java.util.List;

/**
 * Detects whether a connection is using the HA proxy protocol during startup.
 *
 * <p>If a request has a matching header prefix then it decodes using the proxy protocol and sets
 * the public address; otherwise, the request is passed through as a normal CQL protocol message.
 *
 * <p>This cannot use the default logic from {@link HAProxyMessageDecoder#detectProtocol(ByteBuf)}
 * because that uses 12 bytes as the minimum header size, but the CQL native protocol's smallest
 * message size is 8-9 bytes depending on the protocol version. For example, if an OPTIONS (9 bytes)
 * request is sent during startup it would cause the connection process to timeout waiting for more
 * bytes.
 */
public class HAProxyProtocolDetectingDecoder extends ByteToMessageDecoder {
  private static final String NAME = "proxyProtocolDetectingDecoder";

  // Neither of these header prefixes overlap with valid CQL protocol message headers. The only
  // valid CQL request opcodes during startup are STARTUP (0x01) and OPTIONS (0x05).

  private static final byte[] BINARY_PREFIX = {
    (byte) 0x0D, // Not a supported protocol version
    (byte) 0x0A,
    (byte) 0x0D,
    (byte) 0x0A,
    (byte) 0x00, // Invalid request opcode
    (byte) 0x0D,
    (byte) 0x0A,
    (byte) 0x51,
  };

  private static final byte[] TEXT_PREFIX = {
    (byte) 'P', // Not a supported protocol version
    (byte) 'R',
    (byte) 'O',
    (byte) 'X',
    (byte) 'Y', // Invalid request opcode
  };

  private static final int NUM_HEADER_BYTES = Math.max(BINARY_PREFIX.length, TEXT_PREFIX.length);

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
    if (in.readableBytes() < NUM_HEADER_BYTES) { // Wait for more bytes
      return;
    }

    int idx = in.readerIndex();

    if (match(BINARY_PREFIX, in, idx) || match(TEXT_PREFIX, in, idx)) { // HA proxy protocol header
      ctx.pipeline().replace(this, NAME, new HAProxyMessageDecoder());
      ctx.pipeline()
          .addAfter(
              NAME,
              "proxyProtocolMessage",
              new SimpleChannelInboundHandler<HAProxyMessage>() {
                @Override
                protected void channelRead0(ChannelHandlerContext ctx, HAProxyMessage msg) {
                  Attribute<ProxyInfo> attrProxy = ctx.channel().attr(ProxyInfo.attributeKey);
                  attrProxy.set(
                      new ProxyInfo(
                          new InetSocketAddress(msg.destinationAddress(), msg.destinationPort()),
                          new InetSocketAddress(msg.sourceAddress(), msg.sourcePort())));
                }
              });
    } else {
      ctx.pipeline().remove(this);
    }
  }

  private static boolean match(byte[] prefix, ByteBuf buffer, int idx) {
    for (int i = 0; i < prefix.length; i++) {
      final byte b = buffer.getByte(idx + i);
      if (b != prefix[i]) {
        return false;
      }
    }
    return true;
  }
}
