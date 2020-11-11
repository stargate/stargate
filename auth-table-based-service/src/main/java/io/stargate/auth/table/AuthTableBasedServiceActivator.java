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
package io.stargate.auth.table;

import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthorizationService;
import io.stargate.db.Persistence;
import java.util.Hashtable;
import net.jcip.annotations.GuardedBy;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceEvent;
import org.osgi.framework.ServiceListener;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthTableBasedServiceActivator implements BundleActivator, ServiceListener {

  private static final Logger log = LoggerFactory.getLogger(AuthTableBasedServiceActivator.class);

  private BundleContext context;
  private ServiceReference persistenceReference;
  private ServiceRegistration<?> authnRegistration;
  private ServiceRegistration<?> authzRegistration;
  static Hashtable<String, String> props = new Hashtable<>();
  static String PERSISTENCE_IDENTIFIER =
      System.getProperty("stargate.persistence_id", "CassandraPersistence");

  public static final String AUTH_TABLE_IDENTIFIER = "AuthTableBasedService";

  static {
    props.put("AuthIdentifier", AUTH_TABLE_IDENTIFIER);
  }

  @GuardedBy("this")
  private final AuthnTableBasedService authnTableBasedService = new AuthnTableBasedService();

  @GuardedBy("this")
  private final AuthzTableBasedService authzTableBasedService = new AuthzTableBasedService();

  @Override
  public synchronized void start(BundleContext context) {
    if (AUTH_TABLE_IDENTIFIER
        .equals(System.getProperty("stargate.auth_id", AUTH_TABLE_IDENTIFIER))) {
      this.context = context;
      log.info("Starting authnTableBasedService and authzTableBasedService....");

      try {
        context.addServiceListener(this, String.format("(Identifier=%s)", PERSISTENCE_IDENTIFIER));
      } catch (InvalidSyntaxException ise) {
        throw new RuntimeException(ise);
      }

      persistenceReference = context.getServiceReference(Persistence.class.getName());
      if (persistenceReference != null
          && persistenceReference.getProperty("Identifier").equals(PERSISTENCE_IDENTIFIER)) {
        log.info("Setting persistence in AuthTableBasedServiceActivator");
        this.authnTableBasedService.setPersistence(
            (Persistence) context.getService(persistenceReference));
      }

      if (persistenceReference != null) {
        log.info("Registering authnTableBasedService in AuthTableBasedServiceActivator");
        authnRegistration =
            context.registerService(
                AuthenticationService.class.getName(), authnTableBasedService, props);
      }

      log.info("Registering authzTableBasedService in AuthTableBasedServiceActivator");
      authzRegistration =
          context.registerService(
              AuthorizationService.class.getName(), authzTableBasedService, props);
    }
  }

  @Override
  public void stop(BundleContext context) {
    if (persistenceReference != null) {
      context.ungetService(persistenceReference);
    }
    // Do not need to unregister the service, because the OSGi framework will automatically do so
  }

  @Override
  public synchronized void serviceChanged(ServiceEvent serviceEvent) {
    int type = serviceEvent.getType();
    String[] objectClass = (String[]) serviceEvent.getServiceReference().getProperty("objectClass");
    switch (type) {
      case (ServiceEvent.REGISTERED):
        log.info("Service of type " + objectClass[0] + " registered.");
        Object service = context.getService(serviceEvent.getServiceReference());

        if (service instanceof Persistence) {
          log.info("Setting persistence in RestApiActivator");
          this.authnTableBasedService.setPersistence((Persistence) service);
        }

        if (this.authnTableBasedService.getPersistence() != null && authnRegistration == null) {
          log.info("Registering authnTableBasedService in AuthTableBasedServiceActivator");
          authnRegistration =
              context.registerService(
                  AuthenticationService.class.getName(), authnTableBasedService, props);
        }

        if (authzRegistration == null) {
          log.info("Registering authzTableBasedService in AuthTableBasedServiceActivator");
          authzRegistration =
              context.registerService(
                  AuthorizationService.class.getName(), authzTableBasedService, props);
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
