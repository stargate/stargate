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

import io.stargate.cql.impl.CqlImpl;
import io.stargate.db.Persistence;
import io.stargate.health.metrics.api.Metrics;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.apache.cassandra.config.Config;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceEvent;
import org.osgi.framework.ServiceListener;
import org.osgi.framework.ServiceReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CqlActivator implements BundleActivator, ServiceListener {
  private static final Logger log = LoggerFactory.getLogger(CqlActivator.class);

  private BundleContext context;
  private final CqlImpl cql = new CqlImpl(makeConfig());
  private ServiceReference<?> persistenceReference;
  private ServiceReference<?> metricsReference;
  static String PERSISTENCE_IDENTIFIER =
      System.getProperty("stargate.persistence_id", "CassandraPersistence");

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

  @Override
  public void start(BundleContext context) {
    this.context = context;
    log.info("Starting CQL....");
    synchronized (cql) {
      try {
        context.addServiceListener(
            this,
            String.format(
                "(|(Identifier=%s)(objectClass=%s))",
                PERSISTENCE_IDENTIFIER, Metrics.class.getName()));
      } catch (InvalidSyntaxException ise) {
        throw new RuntimeException(ise);
      }

      if (persistenceReference != null
          && persistenceReference.getProperty("Identifier").equals(PERSISTENCE_IDENTIFIER)) {
        log.info("Setting persistence in CQL");
        this.cql.setPersistence((Persistence<?, ?, ?>) context.getService(persistenceReference));
      }

      metricsReference = context.getServiceReference(Metrics.class.getName());
      if (metricsReference != null) {
        log.info("Setting metrics in CQL");
        this.cql.setMetrics((Metrics) context.getService(metricsReference));
      }

      if (this.cql.getPersistence() != null && this.cql.getMetrics() != null) {
        this.cql.start();
        log.info("Started CQL....");
      }
    }
  }

  @Override
  public void stop(BundleContext context) {
    context.ungetService(persistenceReference);
  }

  @Override
  public void serviceChanged(ServiceEvent serviceEvent) {
    int type = serviceEvent.getType();
    String[] objectClass = (String[]) serviceEvent.getServiceReference().getProperty("objectClass");
    synchronized (cql) {
      switch (type) {
        case (ServiceEvent.REGISTERED):
          log.info("Service of type " + objectClass[0] + " registered.");
          Object service = context.getService(serviceEvent.getServiceReference());

          if (service instanceof Persistence) {
            log.info("Setting persistence in CQL");
            this.cql.setPersistence((Persistence<?, ?, ?>) service);
          } else if (service instanceof Metrics) {
            log.info("Setting metrics in CQL");
            this.cql.setMetrics(((Metrics) service));
          }

          if (this.cql.getPersistence() != null && this.cql.getMetrics() != null) {
            this.cql.start();
            log.info("Started CQL.... (via svc changed)");
          }
          break;
        case (ServiceEvent.UNREGISTERING):
          log.info("Service of type " + objectClass[0] + " unregistered.");
          context.ungetService(serviceEvent.getServiceReference());
          break;
        case (ServiceEvent.MODIFIED):
          // TODO: [doug] 2020-06-15, Mon, 12:58 do something here...
          log.info("Service of type " + objectClass[0] + " modified.");
          break;
        default:
          break;
      }
    }
  }
}
