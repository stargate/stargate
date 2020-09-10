package io.stargate.auth.table;

import java.util.Hashtable;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceEvent;
import org.osgi.framework.ServiceListener;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.stargate.auth.AuthenticationService;
import io.stargate.db.Persistence;


public class AuthTableBasedServiceActivator implements BundleActivator, ServiceListener {
    private static final Logger log = LoggerFactory.getLogger(AuthTableBasedServiceActivator.class);

    private BundleContext context;
    private final AuthTableBasedService authTableBasedService = new AuthTableBasedService();
    private ServiceReference persistenceReference;
    private ServiceRegistration<?> registration;
    static Hashtable<String, String> props = new Hashtable<>();
    static String PERSISTENCE_IDENTIFIER = System.getProperty("stargate.persistence_id", "CassandraPersistence");

    static {
        props.put("AuthIdentifier", "AuthTableBasedService");
    }

    @Override
    public void start(BundleContext context) {
        this.context = context;
        log.info("Starting authTableBasedService....");

        synchronized (authTableBasedService) {
            try {
                context.addServiceListener(this, String.format("(Identifier=%s)", PERSISTENCE_IDENTIFIER));
            } catch (InvalidSyntaxException ise) {
                throw new RuntimeException(ise);
            }

            persistenceReference = context.getServiceReference(Persistence.class.getName());
            if (persistenceReference != null && persistenceReference.getProperty("Identifier").equals(PERSISTENCE_IDENTIFIER)) {
                log.info("Setting persistence in AuthTableBasedServiceActivator");
                this.authTableBasedService.setPersistence((Persistence)context.getService(persistenceReference));
            }

            if (persistenceReference != null) {
                log.info("Registering authTableBasedService in AuthTableBasedServiceActivator");
                registration = context.registerService(AuthenticationService.class.getName(), authTableBasedService, props);
            }
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
    public void serviceChanged(ServiceEvent serviceEvent) {
        int type = serviceEvent.getType();
        String[] objectClass = (String[]) serviceEvent.getServiceReference().getProperty("objectClass");
        synchronized (authTableBasedService) {
            switch (type) {
                case (ServiceEvent.REGISTERED):
                    log.info("Service of type " + objectClass[0] + " registered.");
                    Object service = context.getService(serviceEvent.getServiceReference());

                    if (service instanceof Persistence) {
                        log.info("Setting persistence in RestApiActivator");
                        this.authTableBasedService.setPersistence((Persistence) service);
                    }

                    if (this.authTableBasedService.getPersistence() != null && registration == null) {
                        log.info("Registering authTableBasedService in AuthTableBasedServiceActivator");
                        registration = context.registerService(AuthenticationService.class.getName(), authTableBasedService, props);
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
