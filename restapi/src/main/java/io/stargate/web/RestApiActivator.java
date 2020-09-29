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
package io.stargate.web;

import io.stargate.auth.AuthenticationService;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.db.Persistence;
import io.stargate.web.impl.WebImpl;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceEvent;
import org.osgi.framework.ServiceListener;
import org.osgi.framework.ServiceReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Activator for the restapi bundle */
public class RestApiActivator implements BundleActivator, ServiceListener {
  private static final Logger log = LoggerFactory.getLogger(RestApiActivator.class);

  private BundleContext context;
  private final WebImpl web = new WebImpl();
  private ServiceReference persistenceReference;
  private ServiceReference authenticationReference;
  private ServiceReference<?> metricsReference;

  static String AUTH_IDENTIFIER = System.getProperty("stargate.auth_id", "AuthTableBasedService");
  static String PERSISTENCE_IDENTIFIER =
      System.getProperty("stargate.persistence_id", "CassandraPersistence");

  @Override
  public void start(BundleContext context) throws InvalidSyntaxException {
    this.context = context;
    log.info("Starting restapi....");
    synchronized (web) {
      try {
        String authFilter = String.format("(AuthIdentifier=%s)", AUTH_IDENTIFIER);
        String persistenceFilter = String.format("(Identifier=%s)", PERSISTENCE_IDENTIFIER);
        String metricsFilter = String.format("(objectClass=%s)", Metrics.class.getName());
        context.addServiceListener(
            this, String.format("(|%s%s%s)", persistenceFilter, authFilter, metricsFilter));
      } catch (InvalidSyntaxException ise) {
        throw new RuntimeException(ise);
      }

      ServiceReference[] refs =
          context.getServiceReferences(AuthenticationService.class.getName(), null);
      if (refs != null) {
        for (ServiceReference ref : refs) {
          // Get the service object.
          Object service = context.getService(ref);
          if (service instanceof AuthenticationService
              && ref.getProperty("AuthIdentifier") != null
              && ref.getProperty("AuthIdentifier").equals(AUTH_IDENTIFIER)) {
            log.info("Setting authenticationService in RestApiActivator");
            this.web.setAuthenticationService((AuthenticationService) service);
            break;
          }
        }
      }

      persistenceReference = context.getServiceReference(Persistence.class.getName());
      if (persistenceReference != null
          && persistenceReference.getProperty("Identifier").equals(PERSISTENCE_IDENTIFIER)) {
        log.info("Setting persistence in RestApiActivator");
        this.web.setPersistence((Persistence) context.getService(persistenceReference));
      }

      metricsReference = context.getServiceReference(Metrics.class.getName());
      if (metricsReference != null) {
        log.info("Setting metrics in RestApiActivator");
        this.web.setMetrics((Metrics) context.getService(metricsReference));
      }

      if (this.web.getPersistence() != null
          && this.web.getAuthenticationService() != null
          && this.web.getMetrics() != null) {
        try {
          this.web.start();
          log.info("Started restapi....");
        } catch (Exception e) {
          log.error("Failed", e);
        }
      }
    }
  }

  @Override
  public void stop(BundleContext context) {
    if (persistenceReference != null) {
      context.ungetService(persistenceReference);
    }

    if (authenticationReference != null) {
      context.ungetService(authenticationReference);
    }
  }

  @Override
  public void serviceChanged(ServiceEvent serviceEvent) {
    int type = serviceEvent.getType();
    String[] objectClass = (String[]) serviceEvent.getServiceReference().getProperty("objectClass");
    synchronized (web) {
      switch (type) {
        case (ServiceEvent.REGISTERED):
          log.info("Service of type " + objectClass[0] + " registered.");
          Object service = context.getService(serviceEvent.getServiceReference());

          if (service instanceof Persistence) {
            log.info("Setting persistence in RestApiActivator");
            this.web.setPersistence((Persistence) service);
          } else if (service instanceof AuthenticationService
              && serviceEvent.getServiceReference().getProperty("AuthIdentifier") != null
              && serviceEvent
                  .getServiceReference()
                  .getProperty("AuthIdentifier")
                  .equals(AUTH_IDENTIFIER)) {
            log.info("Setting authenticationService in RestApiActivator");
            this.web.setAuthenticationService((AuthenticationService) service);
          } else if (service instanceof Metrics) {
            log.info("Setting metrics in RestApiActivator");
            this.web.setMetrics(((Metrics) service));
          }

          if (this.web.getPersistence() != null
              && this.web.getAuthenticationService() != null
              && this.web.getMetrics() != null) {
            try {
              this.web.start();
              log.info("Started restapi.... (via svc changed)");
            } catch (Exception e) {
              log.error("Failed", e);
            }
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
