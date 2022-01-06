/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.stargate.transport.internal;

import io.netty.channel.Channel;
import io.netty.handler.ssl.SslHandler;
import io.stargate.auth.AuthenticationService;
import io.stargate.db.Authenticator;
import io.stargate.db.ClientInfo;
import io.stargate.db.Persistence;
import java.net.InetSocketAddress;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.security.cert.X509Certificate;
import javax.validation.constraints.NotNull;
import org.apache.cassandra.stargate.metrics.ClientMetrics;
import org.apache.cassandra.stargate.transport.ProtocolException;
import org.apache.cassandra.stargate.transport.ProtocolVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An "external" connection from a regular Stargate CQL client.
 *
 * <p>In this mode, authentication occurs at initialization time, using the regular CQL SASL
 * mechanism.
 *
 * <p>Messages are proxied to exactly one persistence connection, that shares the same lifecycle as
 * this object.
 */
public class ExternalServerConnection extends ServerConnection {

  private static final Logger logger = LoggerFactory.getLogger(ServerConnection.class);

  private volatile Authenticator.SaslNegotiator saslNegotiator;
  private final ClientInfo clientInfo;
  private final Persistence.Connection persistenceConnection;
  private final AuthenticationService authentication;

  ExternalServerConnection(
      Channel channel,
      int boundPort,
      ProxyInfo proxyInfo,
      ProtocolVersion version,
      CqlServer server) {
    this(
        channel,
        proxyInfo,
        version,
        server.connectionTracker,
        server.persistence,
        server.authentication,
        getClientInfo(channel, boundPort, proxyInfo));
  }

  private ExternalServerConnection(
      Channel channel,
      ProxyInfo proxyInfo,
      ProtocolVersion version,
      Tracker tracker,
      Persistence persistence,
      AuthenticationService authentication,
      ClientInfo clientInfo) {
    super(
        channel,
        version,
        tracker,
        ClientMetrics.instance.connectionMetrics(clientInfo),
        persistence);
    this.clientInfo = clientInfo;
    this.persistenceConnection = persistence.newConnection(clientInfo);

    if (proxyInfo != null) this.persistenceConnection.setCustomProperties(proxyInfo.toHeaders());

    this.authentication = authentication;
    this.stage = ConnectionStage.ESTABLISHED;
  }

  @NotNull
  private static ClientInfo getClientInfo(Channel channel, int boundPort, ProxyInfo proxyInfo) {
    return new ClientInfo(
        proxyInfo != null ? proxyInfo.sourceAddress : (InetSocketAddress) channel.remoteAddress(),
        boundPort,
        proxyInfo != null ? proxyInfo.destinationAddress : null);
  }

  public ClientInfo clientInfo() {
    return clientInfo;
  }

  public Persistence.Connection persistenceConnection() {
    return persistenceConnection;
  }

  @Override
  void validateNewMessage(Message.Type type, ProtocolVersion version) {
    switch (stage) {
      case ESTABLISHED:
        if (type != Message.Type.STARTUP && type != Message.Type.OPTIONS)
          throw new ProtocolException(
              String.format("Unexpected message %s, expecting STARTUP or OPTIONS", type));
        break;
      case AUTHENTICATING:
        // Support both SASL auth from protocol v2 and the older style Credentials auth from v1
        if (type != Message.Type.AUTH_RESPONSE && type != Message.Type.CREDENTIALS)
          throw new ProtocolException(
              String.format(
                  "Unexpected message %s, expecting %s",
                  type, version == ProtocolVersion.V1 ? "CREDENTIALS" : "SASL_RESPONSE"));
        break;
      case READY:
        if (type == Message.Type.STARTUP)
          throw new ProtocolException(
              "Unexpected message STARTUP, the connection is already initialized");
        break;
      default:
        throw new AssertionError();
    }
  }

  @Override
  void applyStateTransition(Message.Type requestType, Message.Type responseType) {
    switch (stage) {
      case ESTABLISHED:
        if (requestType == Message.Type.STARTUP) {
          if (responseType == Message.Type.AUTHENTICATE) stage = ConnectionStage.AUTHENTICATING;
          else if (responseType == Message.Type.READY) stage = ConnectionStage.READY;
        }
        break;
      case AUTHENTICATING:
        // Support both SASL auth from protocol v2 and the older style Credentials auth from v1
        assert requestType == Message.Type.AUTH_RESPONSE || requestType == Message.Type.CREDENTIALS;

        if (responseType == Message.Type.READY || responseType == Message.Type.AUTH_SUCCESS) {
          stage = ConnectionStage.READY;
          // we won't use the authenticator again, null it so that it can be GC'd
          saslNegotiator = null;
        }
        break;
      case READY:
        break;
      default:
        throw new AssertionError();
    }
  }

  public Authenticator.SaslNegotiator getSaslNegotiator() {
    if (saslNegotiator == null) {
      Authenticator.SaslNegotiator negotiator =
          persistenceConnection
              .persistence()
              .getAuthenticator()
              .newSaslNegotiator(clientInfo.remoteAddress().getAddress(), certificates());

      saslNegotiator =
          (authentication == null)
              ? negotiator
              : authentication.getSaslNegotiator(negotiator, clientInfo);
    }
    return saslNegotiator;
  }

  private X509Certificate[] certificates() {
    SslHandler sslHandler = (SslHandler) channel().pipeline().get("ssl");
    X509Certificate[] certificates = null;

    if (sslHandler != null) {
      try {
        certificates = sslHandler.engine().getSession().getPeerCertificateChain();
      } catch (SSLPeerUnverifiedException e) {
        logger.error("Failed to get peer certificates for peer {}", channel().remoteAddress(), e);
      }
    }
    return certificates;
  }
}
