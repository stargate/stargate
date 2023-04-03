package io.stargate.it.proxy;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.haproxy.HAProxyCommand;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyMessageEncoder;
import io.netty.handler.codec.haproxy.HAProxyProtocolVersion;
import io.netty.handler.codec.haproxy.HAProxyProxiedProtocol;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A small TCP proxy that implements HAProxy protocol.
 *
 * <p>It only support forwarding traffic to a single backend server. It's based on the
 * "HexDumpProxy" example provided by Netty.
 *
 * @see <a
 *     href="https://www.haproxy.org/download/1.8/doc/proxy-protocol.txt">https://www.haproxy.org/download/1.8/doc/proxy-protocol.txt</a>
 */
public class TcpProxy implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(TcpProxy.class);

  private final EventLoopGroup bossGroup = new NioEventLoopGroup(1);
  private final EventLoopGroup workerGroup = new NioEventLoopGroup();
  private final Channel serverChannel;

  private TcpProxy(
      @NotNull InetSocketAddress localAddress, @NotNull InetSocketAddress remoteAddress)
      throws InterruptedException {
    ServerBootstrap b = new ServerBootstrap();
    serverChannel =
        b.group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .childHandler(new FrontendInitializer(remoteAddress))
            .childOption(ChannelOption.AUTO_READ, false)
            .bind(localAddress)
            .sync()
            .channel();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private InetSocketAddress localAddress;
    private InetSocketAddress remoteAddress;

    private Builder() {}

    /**
     * Sets the address and port for the proxy to bind and listen.
     *
     * @param address the proxy's listening address
     * @param port the proxy's port
     * @return the builder to facilitate chaining
     */
    public Builder localAddress(@NotNull String address, int port) {
      return localAddress(new InetSocketAddress(address, port));
    }

    /**
     * @see #localAddress(String, int)
     * @param address the proxy's listening address and port
     * @return the builder to facilitate chaining
     */
    public Builder localAddress(@NotNull InetSocketAddress address) {
      this.localAddress = address;
      return this;
    }

    /**
     * Sets the upstream address and port for the proxy.
     *
     * @param address the address to of the upstream service
     * @param port the port of the upstream service
     * @return the builder to facilitate chaining
     */
    public Builder remoteAddress(@NotNull String address, int port) {
      return remoteAddress(new InetSocketAddress(address, port));
    }

    /**
     * @see #remoteAddress(String, int)
     * @param address the socket address of the upstream service
     * @return the builder to facilitate chaining
     */
    public Builder remoteAddress(@NotNull InetSocketAddress address) {
      this.remoteAddress = address;
      return this;
    }

    /**
     * Constructs a new TCP proxy.
     *
     * <p>Waits for the connection to bind and start listening for connections before returning.
     *
     * @return a running TCP proxy
     * @throws InterruptedException
     */
    public TcpProxy build() throws InterruptedException {
      return new TcpProxy(localAddress, remoteAddress);
    }
  }

  private static class FrontendInitializer extends ChannelInitializer<SocketChannel> {
    private final InetSocketAddress remoteAddress;

    public FrontendInitializer(InetSocketAddress remoteAddress) {
      this.remoteAddress = remoteAddress;
    }

    @Override
    public void initChannel(SocketChannel ch) {
      ch.pipeline().addLast(new FrontendHandler(remoteAddress));
    }
  }

  private static class FrontendHandler extends ChannelInboundHandlerAdapter {
    private final InetSocketAddress remoteAddress;

    private volatile Channel outboundChannel;

    public FrontendHandler(InetSocketAddress remoteAddress) {
      this.remoteAddress = remoteAddress;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
      final Channel inboundChannel = ctx.channel();

      // Start the connection attempt.
      Bootstrap b = new Bootstrap();
      b.group(inboundChannel.eventLoop())
          .channel(ctx.channel().getClass())
          .handler(new BackendHandler(inboundChannel))
          .option(ChannelOption.AUTO_READ, false);
      ChannelFuture f = b.connect(remoteAddress);
      outboundChannel = f.channel();
      f.addListener(
          (ChannelFutureListener)
              future -> {
                if (future.isSuccess()) {
                  // Connection complete start to read first data
                  inboundChannel.read();
                  LOG.info("Successfully connected to backend address {}", remoteAddress);
                } else {
                  // Close the connection if the connection attempt has failed.
                  inboundChannel.close();
                  if (future.cause() != null) {
                    LOG.error(
                        "Unable to connect to backend address {}: {}",
                        remoteAddress,
                        future.cause());
                  } else {
                    LOG.error(
                        "Connection canceled to backend address {}: {}",
                        remoteAddress,
                        future.cause());
                  }
                }
              });
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) {
      if (outboundChannel.isActive()) {
        outboundChannel
            .writeAndFlush(msg)
            .addListener(
                (ChannelFutureListener)
                    future -> {
                      if (future.isSuccess()) {
                        // Was able to flush out data, start to read the next chunk
                        ctx.channel().read();
                      } else {
                        future.channel().close();
                      }
                    });
      }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
      if (outboundChannel != null) {
        closeOnFlush(outboundChannel);
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      cause.printStackTrace();
      closeOnFlush(ctx.channel());
    }

    /** Closes the specified channel after all queued write requests are flushed. */
    static void closeOnFlush(Channel ch) {
      if (ch.isActive()) {
        ch.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
      }
    }
  }

  private static class BackendHandler extends ChannelInboundHandlerAdapter {
    private final Channel inboundChannel;
    private boolean finishedHeader = false;

    public BackendHandler(Channel inboundChannel) {
      this.inboundChannel = inboundChannel;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
      ctx.pipeline().addBefore(ctx.name(), "proxyProtocol", HAProxyMessageEncoder.INSTANCE);

      InetSocketAddress srcAddress = (InetSocketAddress) inboundChannel.remoteAddress();
      InetAddress src = srcAddress.getAddress();
      InetSocketAddress destAddress = (InetSocketAddress) inboundChannel.localAddress();
      InetAddress dest = destAddress.getAddress();

      ctx.writeAndFlush(
          new HAProxyMessage(
              HAProxyProtocolVersion.V1,
              HAProxyCommand.PROXY,
              destAddress.getAddress() instanceof Inet4Address
                  ? HAProxyProxiedProtocol.TCP4
                  : HAProxyProxiedProtocol.TCP6,
              src.getHostAddress(),
              dest.getHostAddress(),
              srcAddress.getPort(),
              destAddress.getPort()));
      ctx.read();
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) {
      if (!finishedHeader) {
        ctx.pipeline().remove(HAProxyMessageEncoder.class);
        finishedHeader = true;
      }
      inboundChannel
          .writeAndFlush(msg)
          .addListener(
              (ChannelFutureListener)
                  future -> {
                    if (future.isSuccess()) {
                      ctx.channel().read();
                    } else {
                      future.channel().close();
                    }
                  });
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
      FrontendHandler.closeOnFlush(inboundChannel);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      cause.printStackTrace();
      FrontendHandler.closeOnFlush(ctx.channel());
    }
  }

  @Override
  public void close() throws InterruptedException {
    if (serverChannel != null) {
      serverChannel.close();
      serverChannel.closeFuture().sync();
    }
    bossGroup.shutdownGracefully();
    workerGroup.shutdownGracefully();
  }
}
