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
import io.stargate.db.metrics.api.ClientInfoMetricsTagProvider;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.cassandra.stargate.config.Config;
import org.apache.cassandra.stargate.metrics.ClientMetrics;
import org.apache.cassandra.stargate.transport.internal.CBUtil;
import org.apache.cassandra.stargate.transport.internal.CqlServer;
import org.apache.cassandra.stargate.transport.internal.TransportDescriptor;
import org.apache.cassandra.utils.NativeLibrary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CqlImpl {

  private static final Logger logger = LoggerFactory.getLogger(CqlImpl.class);

  private Collection<CqlServer> servers = Collections.emptyList();
  private final EventLoopGroup workerGroup;

  private final Persistence persistence;
  private final Metrics metrics;
  private final AuthenticationService authentication;
  private final ClientInfoMetricsTagProvider clientInfoTagProvider;

  public CqlImpl(
      Config config,
      Persistence persistence,
      Metrics metrics,
      AuthenticationService authentication,
      ClientInfoMetricsTagProvider clientInfoTagProvider) {
    TransportDescriptor.daemonInitialization(config);

    // Please see the comment on setUnsetValue().
    CBUtil.setUnsetValue(persistence.unsetValue());

    this.persistence = persistence;
    this.metrics = metrics;
    this.authentication = authentication;
    this.clientInfoTagProvider = clientInfoTagProvider;

    if (useEpoll()) {
      workerGroup = new EpollEventLoopGroup();
      logger.info("Netty using native Epoll event loop");
    } else {
      workerGroup = new NioEventLoopGroup();
      logger.info("Netty using Java NIO event loop");
    }
  }

  public void start() {
    int nativePort = TransportDescriptor.getNativeTransportPort();
    int nativePortSSL = TransportDescriptor.getNativeTransportPortSSL();
    InetAddress nativeAddr = TransportDescriptor.getRpcAddress();

    CqlServer.Builder builder =
        new CqlServer.Builder(persistence, authentication)
            .withEventLoopGroup(workerGroup)
            .withHost(nativeAddr);

    if (!TransportDescriptor.getNativeProtocolEncryptionOptions().isEnabled()) {
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

    String additionalPorts = System.getProperty("stargate.cql.additional_ports");
    if (additionalPorts != null) {
      List<CqlServer> additionalServers =
          Arrays.stream(additionalPorts.split("\\s*,\\s*"))
              .map(
                  s -> {
                    int port;
                    try {
                      port = Integer.parseInt(s);
                    } catch (NumberFormatException e) {
                      port = 0;
                    }
                    if (port <= 0) {
                      logger.error(
                          "Invalid port value found in property 'stargate.cql.additional_ports': {}",
                          s);
                    }
                    return port;
                  })
              .filter(p -> p > 0)
              .map(
                  p ->
                      builder
                          .withSSL(
                              TransportDescriptor.getNativeProtocolEncryptionOptions().isEnabled())
                          .withPort(p)
                          .build())
              .collect(Collectors.toList());

      if (!additionalServers.isEmpty()) {
        additionalServers.addAll(servers);
        servers = Collections.unmodifiableList(additionalServers);
      }
    }

    double metricsUpdatePeriodSeconds =
        Double.parseDouble(System.getProperty("stargate.cql.metrics.updatePeriodSeconds", "0.1"));
    ClientMetrics.instance.init(
        servers, metrics.getMeterRegistry(), clientInfoTagProvider, metricsUpdatePeriodSeconds);
    servers.forEach(CqlServer::start);
    persistence.setRpcReady(true);
  }

  public void stop() {
    persistence.setRpcReady(false);
    servers.forEach(CqlServer::stop);
    ClientMetrics.instance.shutdown();
  }

  public static boolean useEpoll() {
    final boolean enableEpoll =
        Boolean.parseBoolean(System.getProperty("stargate.cql.native.epoll.enabled", "true"));

    if (enableEpoll && !Epoll.isAvailable() && NativeLibrary.osType == NativeLibrary.OSType.LINUX) {
      logger.warn("epoll requested but not available", Epoll.unavailabilityCause());
    }

    return enableEpoll && Epoll.isAvailable();
  }
}
