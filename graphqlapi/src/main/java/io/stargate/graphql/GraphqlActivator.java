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
import io.stargate.core.metrics.api.Metrics;
import io.stargate.db.Persistence;
import net.jcip.annotations.GuardedBy;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Filter;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceReference;
import org.osgi.util.tracker.ServiceTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Activator for the web bundle */
public class GraphqlActivator implements BundleActivator {
  private static final Logger LOG = LoggerFactory.getLogger(GraphqlActivator.class);

  private static final String AUTH_IDENTIFIER =
      System.getProperty("stargate.auth_id", "AuthTableBasedService");
  private static final String PERSISTENCE_IDENTIFIER =
      System.getProperty("stargate.persistence_id", "CassandraPersistence");
  private static final String DEPENDENCIES_FILTER =
      String.format(
          "(|(AuthIdentifier=%s)(Identifier=%s)(objectClass=%s))",
          AUTH_IDENTIFIER, PERSISTENCE_IDENTIFIER, Metrics.class.getName());

  @GuardedBy("this")
  private Tracker tracker;

  @GuardedBy("this")
  private WebImpl web;

  @Override
  public synchronized void start(BundleContext context) throws InvalidSyntaxException {
    tracker = new Tracker(context, context.createFilter(DEPENDENCIES_FILTER));
    tracker.open();
  }

  @Override
  public synchronized void stop(BundleContext context) {
    maybeStopService();
    tracker.close();
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

  private class Tracker extends ServiceTracker<Object, Object> {

    private Persistence persistence;
    private Metrics metrics;
    private AuthenticationService authentication;

    public Tracker(BundleContext context, Filter filter) {
      super(context, filter, null);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Object addingService(ServiceReference<Object> ref) {
      Object service = super.addingService(ref);
      if (persistence == null && service instanceof Persistence) {
        LOG.debug("Using backend persistence: {}", ref.getBundle());
        persistence = (Persistence) service;
      } else if (metrics == null && service instanceof Metrics) {
        LOG.debug("Using metrics: {}", ref.getBundle());
        metrics = (Metrics) service;
      } else if (authentication == null && service instanceof AuthenticationService) {
        LOG.debug("Using authentication service: {}", ref.getBundle());
        authentication = (AuthenticationService) service;
      }

      if (persistence != null && metrics != null && authentication != null) {
        maybeStartService(persistence, metrics, authentication);
      }

      return service;
    }
  }
}
