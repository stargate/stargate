package io.grpc.netty.shaded.io.grpc.netty;

import io.grpc.internal.SharedResourcePool;
import io.grpc.netty.shaded.io.netty.channel.Channel;
import io.grpc.netty.shaded.io.netty.channel.ChannelFactory;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.ServerChannel;
import io.grpc.netty.shaded.io.netty.channel.group.ChannelGroup;
import io.grpc.netty.shaded.io.netty.channel.group.DefaultChannelGroup;
import io.grpc.netty.shaded.io.netty.util.concurrent.GlobalEventExecutor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks all the created sessions. It wraps the {@link Utils#DEFAULT_SERVER_CHANNEL_FACTORY} and
 * uses it for session creation. Once it's notified via the {@code closeFilter()} method, it closes
 * all channels matching the predicate.
 */
public class CustomChannelFactory implements ChannelFactory<ServerChannel> {
  private static final Logger logger = LoggerFactory.getLogger(CustomChannelFactory.class);
  private final ChannelFactory<? extends ServerChannel> channelFactory;
  private final List<Channel> channels = new ArrayList<>();

  public CustomChannelFactory() {
    this.channelFactory = Utils.DEFAULT_SERVER_CHANNEL_FACTORY;
  }

  public static EventLoopGroup worker() {
    SharedResourcePool<EventLoopGroup> group =
        SharedResourcePool.forResource(Utils.DEFAULT_WORKER_EVENT_LOOP_GROUP);
    return group.getObject();
  }

  public static EventLoopGroup boss() {
    SharedResourcePool<EventLoopGroup> group =
        SharedResourcePool.forResource(Utils.DEFAULT_BOSS_EVENT_LOOP_GROUP);
    return group.getObject();
  }

  @Override
  public ServerChannel newChannel() {
    ServerChannel serverChannel = channelFactory.newChannel();
    channels.add(serverChannel);
    return serverChannel;
  }

  public void closeFilter(Predicate<Map<String, String>> headerFilter) {
    ChannelGroup allChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    channels.stream()
        .filter(
            channel -> {
              ProxyInfo proxyInfo = channel.attr(ProxyInfo.attributeKey).get();
              logger.info("proxyInfo:" + proxyInfo);
              Map<String, String> headers =
                  proxyInfo != null ? proxyInfo.toHeaders() : Collections.emptyMap();
              logger.info("headers: " + headers);
              return headerFilter.test(headers);
            })
        .forEach(allChannels::add);
    writeToGroup(allChannels);
  }

  /**
   * It writes the {@link GracefulServerCloseCommand} that allows the {@link NettyServerHandler} to
   * close the underlying connection gracefully.
   */
  private void writeToGroup(ChannelGroup allChannels) {
    logger.info("Closing channels, number of channels: {} ", allChannels.size());
    GracefulServerCloseCommand streamClosed = new GracefulServerCloseCommand("Stream closed");
    try {
      allChannels.writeAndFlush(streamClosed).get();
      allChannels.close().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("problem when write GracefulServerCloseCommand", e);
    }
  }
}
