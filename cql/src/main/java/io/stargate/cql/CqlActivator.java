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

import com.google.common.base.Strings;
import io.stargate.auth.AuthenticationService;
import io.stargate.cql.impl.CqlImpl;
import io.stargate.db.Persistence;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.apache.cassandra.config.Config;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Filter;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceReference;
import org.osgi.util.tracker.ServiceTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CqlActivator implements BundleActivator {
  private static final Logger log = LoggerFactory.getLogger(CqlActivator.class);

  private CqlImpl cql;
  private Tracker tracker;

  private static final String AUTH_IDENTIFIER = System.getProperty("stargate.cql_auth_id");
  private static final String PERSISTENCE_IDENTIFIER =
      System.getProperty("stargate.persistence_id", "CassandraPersistence");

  private static final String DEPENDENCIES_FILTER =
      String.format(
          "(|(AuthIdentifier=%s)(Identifier=%s))", AUTH_IDENTIFIER, PERSISTENCE_IDENTIFIER);

  @Override
  public void start(BundleContext context) throws InvalidSyntaxException {
    tracker = new Tracker(context, context.createFilter(DEPENDENCIES_FILTER));
    tracker.open();
  }

  @Override
  public void stop(BundleContext context) {
    maybeStopService();
    tracker.close();
  }

  private synchronized void maybeStartService(
      Persistence persistence, AuthenticationService authentication) {
    if (cql != null) { // Already started
      return;
    }

    cql = new CqlImpl(makeConfig());
    cql.start(persistence, authentication);
    log.info("Starting CQL");
  }

  private synchronized void maybeStopService() {
    CqlImpl c = cql;
    if (c != null) {
      log.info(("Stopping CQL"));
      c.stop();
    }

    cql = null;
  }

  private class Tracker extends ServiceTracker<Object, Object> {
    private Persistence persistence;
    private AuthenticationService authentication;

    public Tracker(BundleContext context, Filter filter) {
      super(context, filter, null);
    }

    @Override
    public Object addingService(ServiceReference<Object> ref) {
      Object service = super.addingService(ref);
      if (persistence == null && service instanceof Persistence) {
        log.info("Using backend persistence: {}", ref.getBundle());
        persistence = (Persistence) service;
      } else if (authentication == null && service instanceof AuthenticationService) {
        log.info("Using authentication service: {}", ref.getBundle());
        authentication = (AuthenticationService) service;
      }

      if (persistence != null && (!requiresAuthentication() || authentication != null)) {
        maybeStartService(persistence, authentication);
      }

      return service;
    }
  }

  private static boolean requiresAuthentication() {
    return !Strings.isNullOrEmpty(AUTH_IDENTIFIER);
  }

  private static Config makeConfig() {
    try {
      String listenAddress =
          System.getProperty(
              "stargate.listen_address", InetAddress.getLocalHost().getHostAddress());
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
