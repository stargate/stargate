package io.grpc.netty.shaded.io.grpc.netty;

import io.grpc.netty.shaded.io.netty.channel.ChannelFactory;
import io.grpc.netty.shaded.io.netty.channel.ServerChannel;

/**
 * Tracks all the created sessions. It wraps the {@link Utils#DEFAULT_SERVER_CHANNEL_FACTORY} and
 * uses it for session creation. Once it's notified via the {@code closeFilter()} method, it closes
 * all channels matching the predicate.
 */
public class CustomChannelFactory implements ChannelFactory<ServerChannel> {
  private final ChannelFactory<? extends ServerChannel> channelFactory;

  public CustomChannelFactory() {
    this.channelFactory = Utils.DEFAULT_SERVER_CHANNEL_FACTORY;
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
    return serverChannel;
  }
}
