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
package io.stargate.api.sql.server;

import io.stargate.api.sql.server.postgres.PGServer;
import io.stargate.auth.AuthenticationService;
import io.stargate.db.Persistence;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Filter;
import org.osgi.framework.ServiceReference;
import org.osgi.util.tracker.ServiceTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlActivator implements BundleActivator {

  private static final Logger log = LoggerFactory.getLogger(SqlActivator.class);

  private static final String PERSISTENCE_IDENTIFIER =
      System.getProperty(
          "stargate.sql.persistence_id",
          System.getProperty("stargate.persistence_id", "CassandraPersistence"));

  private static final String DEPENDENCIES_FILTER =
      String.format(
          "(|(Identifier=%s)(objectClass=%s))",
          PERSISTENCE_IDENTIFIER, AuthenticationService.class.getName());

  private Tracker tracker;
  private PGServer pgServer;

  @Override
  public void start(BundleContext context) throws Exception {
    tracker = new Tracker(context, context.createFilter(DEPENDENCIES_FILTER));
    tracker.open();
  }

  @Override
  public void stop(BundleContext context) {
    stopServer();
    tracker.close();
  }

  private synchronized void stopServer() {
    try {
      PGServer pgServer = this.pgServer;
      if (pgServer != null) {
        log.info("Stopping PostgreSQL protocol handler");
        pgServer.stop();
      }

      this.pgServer = null;
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private synchronized void maybeStart(Persistence persistence, AuthenticationService auth) {
    if (pgServer != null) {
      return;
    }

    pgServer = new PGServer(persistence, auth);
    log.info("Starting PostgreSQL protocol handler");
    pgServer.start();
  }

  private class Tracker extends ServiceTracker<Object, Object> {

    private Persistence persistence;
    private AuthenticationService auth;

    public Tracker(BundleContext context, Filter filter) {
      super(context, filter, null);
    }

    @Override
    public Object addingService(ServiceReference<Object> ref) {
      Object service = super.addingService(ref);
      if (persistence == null && service instanceof Persistence) {
        log.info("Using backend persistence: {}", ref.getBundle());
        persistence = (Persistence) service;
      } else if (auth == null && service instanceof AuthenticationService) {
        log.info("Using authentication service: {}", ref.getBundle());
        auth = (AuthenticationService) service;
      }

      if (persistence != null && auth != null) {
        maybeStart(persistence, auth);
      }

      return service;
    }

    @Override
    public void removedService(ServiceReference<Object> reference, Object service) {
      super.removedService(reference, service);

      if (persistence == service) {
        persistence = null;
      } else if (auth == service) {
        auth = null;
      }

      stopServer();
    }
  }
}
