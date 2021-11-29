package io.grpc.netty.shaded.io.grpc.netty;

import io.grpc.netty.shaded.io.netty.channel.Channel;
import io.grpc.netty.shaded.io.netty.channel.ChannelFactory;
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

  public void closeFilter(Predicate<Map<String, String>> headerFilter) {
    ChannelGroup allChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    channels.stream()
        .filter(
            channel -> {
              ProxyInfo proxyInfo = channel.attr(ProxyInfo.attributeKey).get();
              Map<String, String> headers =
                  proxyInfo != null ? proxyInfo.toHeaders() : Collections.emptyMap();
              return headerFilter.test(headers);
            })
        .forEach(allChannels::add);
    writeToGroup(allChannels);
  }

  private void writeToGroup(ChannelGroup allChannels) {
    GracefulServerCloseCommand streamClosed = new GracefulServerCloseCommand("Stream closed");
    try {
      allChannels.writeAndFlush(streamClosed).get();
      allChannels.close().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("problem when write GracefulServerCloseCommand", e);
    }
  }
}
