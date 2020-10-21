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
package io.stargate.auth.api;

import io.stargate.auth.AuthenticationService;
import io.stargate.auth.api.impl.WebImpl;
import io.stargate.core.metrics.api.Metrics;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceEvent;
import org.osgi.framework.ServiceListener;
import org.osgi.framework.ServiceReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthApiActivator implements BundleActivator, ServiceListener {
  private static final Logger log = LoggerFactory.getLogger(AuthApiActivator.class);

  private BundleContext context;
  private final WebImpl web = new WebImpl();
  private ServiceReference authenticationServiceReference;
  private ServiceReference<?> metricsReference;

  static String AUTH_IDENTIFIER = System.getProperty("stargate.auth_id", "AuthTableBasedService");

  @Override
  public void start(BundleContext context) throws Exception {
    this.context = context;
    log.info("Starting apiServer....");
    synchronized (web) {
      try {
        String authFilter = String.format("(AuthIdentifier=%s)", AUTH_IDENTIFIER);
        String metricsFilter = String.format("(objectClass=%s)", Metrics.class.getName());
        context.addServiceListener(this, String.format("(|%s%s)", authFilter, metricsFilter));
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
            log.info("Setting authenticationService in AuthApiActivator");
            this.web.setAuthenticationService((AuthenticationService) service);
            break;
          }
        }
      }

      metricsReference = context.getServiceReference(Metrics.class.getName());
      if (metricsReference != null) {
        log.info("Setting metrics in AuthApiActivator");
        this.web.setMetrics((Metrics) context.getService(metricsReference));
      }

      if (this.web.getAuthenticationService() != null && this.web.getMetrics() != null) {
        try {
          this.web.start();
          log.info("Started authApiServer....");
        } catch (Exception e) {
          log.error("Failed", e);
        }
      }
    }
  }

  @Override
  public void stop(BundleContext context) {
    if (authenticationServiceReference != null) {
      context.ungetService(authenticationServiceReference);
    }

    if (metricsReference != null) {
      context.ungetService(metricsReference);
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

          if (service instanceof AuthenticationService
              && serviceEvent.getServiceReference().getProperty("AuthIdentifier") != null
              && serviceEvent
                  .getServiceReference()
                  .getProperty("AuthIdentifier")
                  .equals(AUTH_IDENTIFIER)) {
            log.info("Setting authenticationService in AuthApiActivator");
            this.web.setAuthenticationService((AuthenticationService) service);
          } else if (service instanceof Metrics) {
            log.info("Setting metrics in AuthApiActivator");
            this.web.setMetrics(((Metrics) service));
          }

          if (this.web.getAuthenticationService() != null && this.web.getMetrics() != null) {
            try {
              this.web.start();
              log.info("Started authApiServer.... (via svc changed)");
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
