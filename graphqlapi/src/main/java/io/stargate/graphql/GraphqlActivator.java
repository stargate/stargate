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

import io.stargate.auth.AuthenticationService;
import io.stargate.core.activator.BaseActivator;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.db.Persistence;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import net.jcip.annotations.GuardedBy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Activator for the web bundle */
public class GraphqlActivator extends BaseActivator {
  private static final Logger LOG = LoggerFactory.getLogger(GraphqlActivator.class);

  private static final String AUTH_IDENTIFIER =
      System.getProperty("stargate.auth_id", "AuthTableBasedService");
  private static final String PERSISTENCE_IDENTIFIER =
      System.getProperty("stargate.persistence_id", "CassandraPersistence");

  private ServicePointer<AuthenticationService> authentication =
      ServicePointer.create(AuthenticationService.class, "AuthIdentifier", AUTH_IDENTIFIER);
  private ServicePointer<Persistence> persistence =
      ServicePointer.create(Persistence.class, "Identifier", PERSISTENCE_IDENTIFIER);
  private ServicePointer<Metrics> metrics = ServicePointer.create(Metrics.class);

  @GuardedBy("this")
  private WebImpl web;

  public GraphqlActivator() {
    super("GraphQL");
  }

  @Override
  @Nullable
  protected ServiceAndProperties createService() {
    maybeStartService(persistence.getService(), metrics.getService(), authentication.getService());
    return null;
  }

  @Override
  protected void stopService() {
    maybeStopService();
  }

  @Override
  protected List<ServicePointer<?>> dependencies() {
    return Arrays.asList(persistence, metrics, authentication);
  }

  private synchronized void maybeStartService(
      Persistence persistence, Metrics metrics, AuthenticationService authentication) {
    if (web == null) {
      try {
        web = new WebImpl(persistence, metrics, authentication);
        LOG.info("Starting GraphQL");
        web.start();
      } catch (Exception e) {
        LOG.error("Unexpected error while stopping GraphQL", e);
      }
    }
  }

  private synchronized void maybeStopService() {
    if (web != null) {
      try {
        LOG.info("Stopping GraphQL");
        web.stop();
      } catch (Exception e) {
        LOG.error("Unexpected error while stopping GraphQL", e);
      }
    }
  }
}
