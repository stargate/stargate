package io.grpc.netty.shaded.io.grpc.netty;

import static io.stargate.grpc.service.GrpcService.CONNECTION_KEY;

import io.grpc.Context;
import io.grpc.internal.SharedResourcePool;
import io.grpc.netty.shaded.io.netty.channel.Channel;
import io.grpc.netty.shaded.io.netty.channel.ChannelFactory;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.ServerChannel;
import io.grpc.netty.shaded.io.netty.channel.group.ChannelGroup;
import io.grpc.netty.shaded.io.netty.channel.group.DefaultChannelGroup;
import io.grpc.netty.shaded.io.netty.util.concurrent.GlobalEventExecutor;
import io.stargate.grpc.service.GrpcService;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks all the created sessions. It wraps the {@link Utils#DEFAULT_SERVER_CHANNEL_FACTORY} and
 * uses it for session creation. Once it's notified via the {@code closeFilter()} method, it closes
 * all channels matching the predicate.
 */
public class CustomChannelFactory implements ChannelFactory<ServerChannel> {
  public static final Context.Key<Map<String, String>> HEADERS_KEY = Context.key("headers");
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
    System.out.println(
        "Creating a new serverChannel:  "
            + serverChannel
            + " with id: "
            + serverChannel.id()
            + " type: "
            + serverChannel.getClass()
            + " thread: "
            + Thread.currentThread().getName()
            + " class: "
            + Thread.currentThread().getClass());

    System.out.println(
        "newChannel(), CONNECTION_KEY.get(): "
            + CONNECTION_KEY.get()
            + "thread: "
            + Thread.currentThread().getName()
            + " class: "
            + Thread.currentThread().getClass());
    channels.add(serverChannel);
    return serverChannel;
  }

  public void closeFilter(Predicate<Map<String, String>> headerFilter) {
    ChannelGroup allChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    channels.stream()
        .filter(
            channel -> {
              Map<String, String> headersFromContext = GrpcService.HEADERS_KEY.get();
              Map<String, String> headers =
                  headersFromContext != null ? headersFromContext : Collections.emptyMap();
              logger.info(
                  "CustomEventLoopGroup.closeFilter(): nr of channels: "
                      + channels.size()
                      + " headers: "
                      + headers
                      + "Thread: "
                      + Thread.currentThread().getName());
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
    allChannels.writeAndFlush(streamClosed).awaitUninterruptibly();
    allChannels.close().awaitUninterruptibly();
  }
}
