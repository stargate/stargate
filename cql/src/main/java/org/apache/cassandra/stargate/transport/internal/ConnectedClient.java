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

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import io.netty.handler.ssl.SslHandler;
import io.stargate.db.AuthenticatedUser;
import io.stargate.db.ClientInfo;
import io.stargate.db.DriverInfo;
import io.stargate.db.Persistence;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;

public final class ConnectedClient {
  public static final String ADDRESS = "address";
  public static final String USER = "user";
  public static final String VERSION = "version";
  public static final String DRIVER_NAME = "driverName";
  public static final String DRIVER_VERSION = "driverVersion";
  public static final String REQUESTS = "requests";
  public static final String KEYSPACE = "keyspace";
  public static final String SSL = "ssl";
  public static final String CIPHER = "cipher";
  public static final String PROTOCOL = "protocol";

  private static final String UNDEFINED = "undefined";

  private final ExternalServerConnection connection;

  ConnectedClient(ExternalServerConnection connection) {
    this.connection = connection;
  }

  public ConnectionStage stage() {
    return connection.stage();
  }

  public InetSocketAddress remoteAddress() {
    return clientInfo().remoteAddress();
  }

  public Optional<String> username() {
    return persistenceConnection().loggedUser().map(AuthenticatedUser::name);
  }

  public int protocolVersion() {
    return connection.getVersion().asInt();
  }

  public Optional<String> driverName() {
    return clientInfo().driverInfo().map(DriverInfo::name);
  }

  public Optional<String> driverVersion() {
    return clientInfo().driverInfo().flatMap(DriverInfo::version);
  }

  public long requestCount() {
    return connection.requests.getCount();
  }

  public Optional<String> keyspace() {
    return persistenceConnection().usedKeyspace();
  }

  public boolean sslEnabled() {
    return null != sslHandler();
  }

  public Optional<String> sslCipherSuite() {
    SslHandler sslHandler = sslHandler();

    return null != sslHandler
        ? Optional.of(sslHandler.engine().getSession().getCipherSuite())
        : Optional.empty();
  }

  public Optional<String> sslProtocol() {
    SslHandler sslHandler = sslHandler();

    return null != sslHandler
        ? Optional.of(sslHandler.engine().getSession().getProtocol())
        : Optional.empty();
  }

  private ClientInfo clientInfo() {
    return connection.clientInfo();
  }

  private Persistence.Connection persistenceConnection() {
    return connection.persistenceConnection();
  }

  private SslHandler sslHandler() {
    return connection.channel().pipeline().get(SslHandler.class);
  }

  public Map<String, String> asMap() {
    return ImmutableMap.<String, String>builder()
        .put(ADDRESS, remoteAddress().toString())
        .put(USER, username().orElse(UNDEFINED))
        .put(VERSION, String.valueOf(protocolVersion()))
        .put(DRIVER_NAME, driverName().orElse(UNDEFINED))
        .put(DRIVER_VERSION, driverVersion().orElse(UNDEFINED))
        .put(REQUESTS, String.valueOf(requestCount()))
        .put(KEYSPACE, keyspace().orElse(""))
        .put(SSL, Boolean.toString(sslEnabled()))
        .put(CIPHER, sslCipherSuite().orElse(UNDEFINED))
        .put(PROTOCOL, sslProtocol().orElse(UNDEFINED))
        .build();
  }
}
