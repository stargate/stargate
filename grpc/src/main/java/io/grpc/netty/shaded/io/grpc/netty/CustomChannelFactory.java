package io.grpc.netty.shaded.io.grpc.netty;

import io.grpc.netty.shaded.io.netty.channel.Channel;
import io.grpc.netty.shaded.io.netty.channel.ChannelFactory;
import io.grpc.netty.shaded.io.netty.channel.ServerChannel;
import java.util.ArrayList;
import java.util.List;

public class CustomChannelFactory implements ChannelFactory<ServerChannel> {

  private final ChannelFactory<? extends ServerChannel> channelFactory;
  private final List<Channel> channels = new ArrayList<>();

  public CustomChannelFactory() {
    this.channelFactory = Utils.DEFAULT_SERVER_CHANNEL_FACTORY;
  }

  @Override
  public ServerChannel newChannel() {
    ServerChannel serverChannel = channelFactory.newChannel();
    channels.add(serverChannel);
    return serverChannel;
  }
}
