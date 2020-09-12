package io.stargate.graphql;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceEvent;
import org.osgi.framework.ServiceListener;
import org.osgi.framework.ServiceReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.stargate.auth.AuthenticationService;
import io.stargate.db.Persistence;

/**
 * Activator for the web bundle
 */
public class GraphqlActivator implements BundleActivator, ServiceListener {
    private static final Logger log = LoggerFactory.getLogger(GraphqlActivator.class);

    private BundleContext context;
    private ServiceReference persistenceReference;
    private ServiceReference authenticationReference;
    private WebImpl web;

    static String AUTH_IDENTIFIER = System.getProperty("stargate.auth_id", "AuthTableBasedService");
    static String PERSISTENCE_IDENTIFIER = System.getProperty("stargate.persistence_id", "CassandraPersistence");

    @Override
    public void start(BundleContext context) throws InvalidSyntaxException {
        web = new WebImpl();
        this.context = context;
        log.info("Starting graphql....");
        try {
            String authFilter = String.format("(AuthIdentifier=%s)", AUTH_IDENTIFIER);
            String persistenceFilter = String.format("(Identifier=%s)", PERSISTENCE_IDENTIFIER);
            context.addServiceListener(this, String.format("(|%s%s)", persistenceFilter, authFilter));
        } catch (InvalidSyntaxException ise) {
            throw new RuntimeException(ise);
        }

        ServiceReference[] refs = context.getServiceReferences(AuthenticationService.class.getName(), null);
        if (refs != null) {
            for (ServiceReference ref : refs) {
                // Get the service object.
                Object service = context.getService(ref);
                if (service instanceof AuthenticationService && ref.getProperty("AuthIdentifier") != null
                        && ref.getProperty("AuthIdentifier").equals(AUTH_IDENTIFIER)) {
                    log.info("Setting authenticationService in GraphqlActivator");
                    this.web.setAuthenticationService((AuthenticationService)service);
                    break;
                }
            }
        }

        persistenceReference = context.getServiceReference(Persistence.class.getName());
        if (persistenceReference != null && persistenceReference.getProperty("Identifier").equals(PERSISTENCE_IDENTIFIER)) {
            log.info("Setting persistence in GraphqlActivator");
            this.web.setPersistence((Persistence)context.getService(persistenceReference));
        }

        if (this.web.getPersistence() != null && this.web.getAuthenticationService() != null) {
            try {
                this.web.start();
                log.info("Started graphql....");
            } catch (Exception e) {
                log.error("Failed", e);
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
        try {
            web.stop();
        } catch (Exception e) {
            log.error("Failed", e);
        }
    }

    @Override
    public void serviceChanged(ServiceEvent serviceEvent) {
        int type = serviceEvent.getType();
        String[] objectClass = (String[]) serviceEvent.getServiceReference().getProperty("objectClass");
        switch (type) {
            case (ServiceEvent.REGISTERED):
                log.info("Service of type " + objectClass[0] + " registered.");
                Object service = context.getService(serviceEvent.getServiceReference());

                if (service instanceof Persistence) {
                    log.info("Setting persistence in GraphqlActivator");
                    this.web.setPersistence((Persistence) service);
                } else if (service instanceof AuthenticationService && serviceEvent.getServiceReference().getProperty("AuthIdentifier") != null
                        && serviceEvent.getServiceReference().getProperty("AuthIdentifier").equals(AUTH_IDENTIFIER)) {
                    log.info("Setting authenticationService in GraphqlActivator");
                    this.web.setAuthenticationService((AuthenticationService)service);
                }

                if (this.web.getPersistence() != null && this.web.getAuthenticationService() != null) {
                    log.info("Started graphql...." + service);
                    try {
                        web.start();
                    } catch (Exception e) {
                        log.error("Failed", e);
                    }
                }
                break;
            case (ServiceEvent.UNREGISTERING):
                try {
                    web.stop();
                } catch (Exception e) {
                    log.error("Failed", e);
                }
                break;
        }
    }
}
