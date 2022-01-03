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
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.stargate.metrics.ClientMetrics;
import org.apache.cassandra.stargate.transport.internal.CBUtil;
import org.apache.cassandra.stargate.transport.internal.CqlServer;
import org.apache.cassandra.stargate.transport.internal.TransportDescriptor;
import org.apache.cassandra.utils.NativeLibrary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CqlImpl {

  private static final Logger logger = LoggerFactory.getLogger(CqlImpl.class);

  private static final String ADDITIONAL_PORTS_PROPERTY = "stargate.cql.additional_ports";
  private static final String INTERNAL_ADDRESS_PROPERTY = "stargate.cql.internal_listen_address";
  private static final String INTERNAL_PORT_PROPERTY = "stargate.cql.internal_port";

  private volatile Collection<CqlServer> servers = Collections.emptyList();
  private final EventLoopGroup workerGroup;

  private final TransportDescriptor transportDescriptor;
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

    this.transportDescriptor = new TransportDescriptor(config);

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

    double metricsUpdatePeriodSeconds =
        Double.parseDouble(System.getProperty("stargate.cql.metrics.updatePeriodSeconds", "0.1"));

    servers = Collections.unmodifiableList(buildServers());

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

  private List<CqlServer> buildServers() {
    List<CqlServer> result = new ArrayList<>();

    int nativePort = transportDescriptor.getNativeTransportPort();
    int nativePortSSL = transportDescriptor.getNativeTransportPortSSL();
    InetAddress nativeAddr = transportDescriptor.getRpcAddress();

    CqlServer.Builder builder =
        new CqlServer.Builder(transportDescriptor, persistence, authentication)
            .withEventLoopGroup(workerGroup)
            .withHost(nativeAddr);

    transportDescriptor.getNativeProtocolEncryptionOptions().applyConfig();
    if (!transportDescriptor.getNativeProtocolEncryptionOptions().isEnabled()) {
      result.add(builder.withSSL(false).withPort(nativePort).build());
    } else {
      if (nativePort != nativePortSSL) {
        // user asked for dedicated ssl port for supporting both non-ssl and ssl connections
        result.add(builder.withSSL(false).withPort(nativePort).build());
        result.add(builder.withSSL(true).withPort(nativePortSSL).build());
      } else {
        // ssl only mode using configured native port
        result.add(builder.withSSL(true).withPort(nativePort).build());
      }
    }

    List<Integer> additionalPorts = getAdditionalPorts();
    for (Integer port : additionalPorts) {
      result.add(builder.withSSL(false).withPort(port).build());
    }

    InetAddress internalAddress = getInternalAddress();
    Integer internalPort = getInternalPort(additionalPorts);
    result.add(
        new CqlServer.Builder(transportDescriptor.toInternal(), persistence, authentication)
            .withEventLoopGroup(workerGroup)
            .withHost(internalAddress)
            .withSSL(false)
            .withPort(internalPort)
            .build());

    return result;
  }

  private List<Integer> getAdditionalPorts() {
    String property = System.getProperty(ADDITIONAL_PORTS_PROPERTY);
    if (property == null) {
      return Collections.emptyList();
    } else {
      List<Integer> ports = new ArrayList<>();
      for (String s : property.split("\\s*,\\s*")) {
        int port;
        try {
          port = Integer.parseInt(s);
        } catch (NumberFormatException e) {
          port = 0;
        }
        if (port <= 0) {
          logger.error(
              "Invalid port value found in property '{}': {}", ADDITIONAL_PORTS_PROPERTY, s);
        } else if (port == transportDescriptor.getNativeTransportPort()) {
          logger.error(
              "Value in property '{}}' is already used as the main port: {}",
              ADDITIONAL_PORTS_PROPERTY,
              s);
        } else if (port == transportDescriptor.getNativeTransportPortSSL()) {
          logger.error(
              "Value in property '{}}' is already used " + "as the main SSL port: {}",
              ADDITIONAL_PORTS_PROPERTY,
              s);
        } else if (ports.contains(port)) {
          logger.error(
              "Duplicate port value found in property '{}': {}", s, ADDITIONAL_PORTS_PROPERTY);
        } else {
          ports.add(port);
        }
      }
      return ports;
    }
  }

  private InetAddress getInternalAddress() {
    String property = System.getProperty(INTERNAL_ADDRESS_PROPERTY);
    if (property == null) {
      return transportDescriptor.getRpcAddress();
    } else {
      try {
        return InetAddress.getByName(property);
      } catch (UnknownHostException e) {
        throw new ConfigurationException(
            String.format("Unknown host in property '%s' %s", INTERNAL_ADDRESS_PROPERTY, property),
            false);
      }
    }
  }

  private Integer getInternalPort(List<Integer> additionalPorts) {
    int internalPort = Integer.getInteger(INTERNAL_PORT_PROPERTY, 9044);
    if (internalPort == transportDescriptor.getNativeTransportPort()) {
      throw new ConfigurationException(
          String.format(
              "Invalid value for property '%s' (%d): already used as the external port",
              INTERNAL_PORT_PROPERTY, internalPort));
    } else if (internalPort == transportDescriptor.getNativeTransportPortSSL()) {
      throw new ConfigurationException(
          String.format(
              "Invalid value for property '%s' (%d): already used as the external SSL port",
              INTERNAL_PORT_PROPERTY, internalPort));
    } else if (additionalPorts.contains(internalPort)) {
      throw new ConfigurationException(
          String.format(
              "Invalid value for property '%s' (%d): already used in property '%s'",
              INTERNAL_PORT_PROPERTY, internalPort, ADDITIONAL_PORTS_PROPERTY));
    }
    return internalPort;
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
