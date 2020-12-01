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
package io.stargate.cql;

import io.stargate.auth.AuthenticationService;
import io.stargate.core.activator.BaseActivator;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.cql.impl.CqlImpl;
import io.stargate.db.Persistence;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import org.apache.cassandra.config.Config;
import org.jetbrains.annotations.Nullable;

public class CqlActivator extends BaseActivator {
  private CqlImpl cql;
  private final ServicePointer<Metrics> metrics = ServicePointer.create(Metrics.class);
  private final ServicePointer<AuthenticationService> authentication =
      ServicePointer.create(
          AuthenticationService.class,
          "AuthIdentifier",
          System.getProperty("stargate.auth_id", "AuthTableBasedService"));
  private final ServicePointer<Persistence> persistence =
      ServicePointer.create(
          Persistence.class,
          "Identifier",
          System.getProperty("stargate.persistence_id", "CassandraPersistence"));

  private static final boolean USE_AUTH_SERVICE =
      Boolean.parseBoolean(System.getProperty("stargate.cql_use_auth_service", "false"));

  public CqlActivator() {
    super("CQL");
  }

  @Nullable
  @Override
  protected ServiceAndProperties createService() {
    if (cql != null) { // Already started
      return null;
    }
    cql = new CqlImpl(makeConfig(), persistence.get(), metrics.get(), authentication.get());
    cql.start();

    return null;
  }

  @Override
  protected void stopService() {
    if (cql == null) { // Not started
      return;
    }
    cql.stop();
    cql = null;
  }

  @Override
  protected List<ServicePointer<?>> dependencies() {
    if (USE_AUTH_SERVICE) {
      return Arrays.asList(metrics, persistence, authentication);
    } else {
      return Arrays.asList(metrics, persistence);
    }
  }

  private static Config makeConfig() {
    try {
      String listenAddress =
          System.getProperty(
              "stargate.listen_address", InetAddress.getLocalHost().getHostAddress());

      if (!Boolean.getBoolean("stargate.bind_to_listen_address")) listenAddress = "0.0.0.0";

      Integer cqlPort = Integer.getInteger("stargate.cql_port", 9042);

      Config c = new Config();

      c.rpc_address = listenAddress;
      c.native_transport_port = cqlPort;

      return c;
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }
}
