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
package io.stargate.cql.impl;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.stargate.auth.AuthenticationService;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.db.Persistence;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.stargate.metrics.ClientMetrics;
import org.apache.cassandra.stargate.transport.internal.Server;
import org.apache.cassandra.stargate.transport.internal.TransportDescriptor;
import org.apache.cassandra.utils.NativeLibrary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CqlImpl {
  private static final Logger logger = LoggerFactory.getLogger(CqlImpl.class);

  private Collection<Server> servers = Collections.emptyList();
  private final EventLoopGroup workerGroup;

  public CqlImpl(Config config) {
    TransportDescriptor.daemonInitialization(config);

    if (useEpoll()) {
      workerGroup = new EpollEventLoopGroup();
      logger.info("Netty using native Epoll event loop");
    } else {
      workerGroup = new NioEventLoopGroup();
      logger.info("Netty using Java NIO event loop");
    }
  }

  public void start(
      Persistence persistence, Metrics metrics, AuthenticationService authentication) {

    int nativePort = TransportDescriptor.getNativeTransportPort();
    int nativePortSSL = TransportDescriptor.getNativeTransportPortSSL();
    InetAddress nativeAddr = TransportDescriptor.getRpcAddress();

    Server.Builder builder =
        new Server.Builder(persistence, authentication)
            .withEventLoopGroup(workerGroup)
            .withHost(nativeAddr);

    if (!TransportDescriptor.getNativeProtocolEncryptionOptions().enabled) {
      servers = Collections.singleton(builder.withSSL(false).withPort(nativePort).build());
    } else {
      if (nativePort != nativePortSSL) {
        // user asked for dedicated ssl port for supporting both non-ssl and ssl connections
        servers =
            Collections.unmodifiableList(
                Arrays.asList(
                    builder.withSSL(false).withPort(nativePort).build(),
                    builder.withSSL(true).withPort(nativePortSSL).build()));
      } else {
        // ssl only mode using configured native port
        servers = Collections.singleton(builder.withSSL(true).withPort(nativePort).build());
      }
    }

    ClientMetrics.instance.init(servers, metrics.getRegistry("cql"));
    servers.forEach(Server::start);
  }

  public void stop() {
    servers.forEach(Server::stop);
  }

  public static boolean useEpoll() {
    final boolean enableEpoll =
        Boolean.parseBoolean(System.getProperty("stargate.cql.native.epoll.enabled", "true"));

    if (enableEpoll && !Epoll.isAvailable() && NativeLibrary.osType == NativeLibrary.OSType.LINUX)
      logger.warn("epoll not available", Epoll.unavailabilityCause());

    return enableEpoll && Epoll.isAvailable();
  }
}
