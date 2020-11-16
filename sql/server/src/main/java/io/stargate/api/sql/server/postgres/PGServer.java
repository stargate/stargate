/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.api.sql.server.postgres;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.stargate.api.sql.server.postgres.transport.MessageDecoder;
import io.stargate.api.sql.server.postgres.transport.MessageEncoder;
import io.stargate.auth.AuthenticationService;
import io.stargate.db.Persistence;
import io.stargate.db.datastore.DataStore;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;
import org.apache.commons.lang3.SystemUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.lang3.concurrent.BasicThreadFactory.Builder;

public class PGServer {
  public static final int PORT = Integer.getInteger("stargate.sql.postgres.port", 5432);

  // TODO: configure / tune Postgres protocol thread pool
  private static final int threadPoolSize = Integer.getInteger("stargate.sql.postgres.threads", 2);

  private static final boolean useEpoll =
      Boolean.parseBoolean(
          System.getProperty(
              "stargate.sql.postgres.epoll", String.valueOf(SystemUtils.IS_OS_LINUX)));

  private static final BasicThreadFactory threadFactory =
      new Builder().namingPattern("pg-protocol-%d").build();

  private final InetSocketAddress socket;
  private final EventLoopGroup workerGroup;
  private final DataStore dataStore;
  private final AuthenticationService authenticator;

  public PGServer(Persistence backend, AuthenticationService authenticator) {
    this(DataStore.create(backend), authenticator, PORT);
  }

  public PGServer(DataStore dataStore, AuthenticationService authenticator, int port) {
    this.dataStore = dataStore;
    this.authenticator = authenticator;

    socket = new InetSocketAddress(port);
    if (useEpoll) {
      workerGroup = new EpollEventLoopGroup(threadPoolSize, threadFactory);
    } else {
      workerGroup = new NioEventLoopGroup(threadPoolSize, threadFactory);
    }
  }

  public void start() {
    ServerBootstrap bootstrap =
        new ServerBootstrap()
            .group(workerGroup)
            .channel(useEpoll ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
            .childOption(ChannelOption.AUTO_READ, true)
            .childOption(ChannelOption.TCP_NODELAY, true)
            .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
            .childHandler(
                new ChannelInitializer<SocketChannel>() {
                  @Override
                  protected void initChannel(SocketChannel ch) {
                    ch.pipeline().addLast(new MessageEncoder());
                    ch.pipeline().addLast(new MessageDecoder());
                    ch.pipeline()
                        .addLast(
                            new MessageDispatcher(new Connection(ch, dataStore, authenticator)));
                  }
                });

    ChannelFuture bindFuture = bootstrap.bind(socket);
    if (!bindFuture.awaitUninterruptibly().isSuccess()) {
      throw new IllegalStateException(
          String.format(
              "Failed to bind port %d on %s.",
              socket.getPort(), socket.getAddress().getHostAddress()),
          bindFuture.cause());
    }
  }

  public void stop() throws ExecutionException, InterruptedException {
    workerGroup.shutdownGracefully().get();
  }
}
