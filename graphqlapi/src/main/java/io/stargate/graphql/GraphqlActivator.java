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
package io.stargate.graphql;

import com.codahale.metrics.health.HealthCheckRegistry;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthorizationService;
import io.stargate.core.activator.BaseActivator;
import io.stargate.core.metrics.api.HttpMetricsTagProvider;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.db.DbActivator;
import io.stargate.db.Persistence;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.graphql.web.DropwizardServer;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import net.jcip.annotations.GuardedBy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Activator for the web bundle */
public class GraphqlActivator extends BaseActivator {

  public static final String MODULE_NAME = "graphqlapi";

  private static final Logger LOG = LoggerFactory.getLogger(GraphqlActivator.class);

  private static final String AUTH_IDENTIFIER =
      System.getProperty("stargate.auth_id", "AuthTableBasedService");
  private static final boolean ENABLE_GRAPHQL_FIRST =
      Boolean.parseBoolean(System.getProperty("stargate.graphql_first.enabled", "true"));
  private static final boolean ENABLE_GRAPHQL_PLAYGROUND =
      !Boolean.getBoolean("stargate.graphql_playground.disabled");

  private final ServicePointer<AuthenticationService> authentication =
      ServicePointer.create(AuthenticationService.class, "AuthIdentifier", AUTH_IDENTIFIER);
  private final ServicePointer<AuthorizationService> authorization =
      ServicePointer.create(AuthorizationService.class, "AuthIdentifier", AUTH_IDENTIFIER);
  private final ServicePointer<Persistence> persistence =
      ServicePointer.create(Persistence.class, "Identifier", DbActivator.PERSISTENCE_IDENTIFIER);
  private final ServicePointer<Metrics> metrics = ServicePointer.create(Metrics.class);
  private final ServicePointer<HttpMetricsTagProvider> httpTagProvider =
      ServicePointer.create(HttpMetricsTagProvider.class);
  private final ServicePointer<HealthCheckRegistry> healthCheckRegistry =
      ServicePointer.create(HealthCheckRegistry.class);

  private final ServicePointer<DataStoreFactory> dataStoreFactory =
      ServicePointer.create(DataStoreFactory.class);

  @GuardedBy("this")
  private DropwizardServer server;

  public GraphqlActivator() {
    super("GraphQL", true);
  }

  @Override
  @Nullable
  protected ServiceAndProperties createService() {
    maybeStartService();
    return null;
  }

  @Override
  protected void stopService() {
    maybeStopService();
  }

  @Override
  protected List<ServicePointer<?>> dependencies() {
    return Arrays.asList(
        persistence,
        metrics,
        httpTagProvider,
        healthCheckRegistry,
        authentication,
        authorization,
        dataStoreFactory);
  }

  private synchronized void maybeStartService() {
    if (server == null) {
      try {
        server =
            new DropwizardServer(
                persistence.get(),
                authentication.get(),
                authorization.get(),
                metrics.get(),
                httpTagProvider.get(),
                dataStoreFactory.get(),
                ENABLE_GRAPHQL_FIRST,
                ENABLE_GRAPHQL_PLAYGROUND);
        LOG.info("Starting GraphQL");
        server.run("server", "graphqlapi-config.yaml");
      } catch (Exception e) {
        LOG.error("Unexpected error while stopping GraphQL", e);
      }
    }
  }

  private synchronized void maybeStopService() {
    if (server != null) {
      try {
        LOG.info("Stopping GraphQL");
        server.stop();
      } catch (Exception e) {
        LOG.error("Unexpected error while stopping GraphQL", e);
      }
    }
  }
}
