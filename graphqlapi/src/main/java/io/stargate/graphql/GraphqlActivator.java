package io.stargate.graphql;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceEvent;
import org.osgi.framework.ServiceListener;
import org.osgi.framework.ServiceReference;

import io.stargate.auth.AuthenticationService;
import io.stargate.coordinator.Coordinator;

/**
 * Activator for the web bundle
 */
public class GraphqlActivator implements BundleActivator, ServiceListener {

    private BundleContext context;
    private ServiceReference coordinatorReference;
    private ServiceReference authenticationReference;
    private WebImpl web;

    static String AUTH_IDENTIFIER = "AuthTableBasedService";

    static {
        String auth = System.getProperty("stargate.auth_id");
        if (auth != null) {
            AUTH_IDENTIFIER = auth;
        }
    }

    @Override
    public void start(BundleContext context) throws InvalidSyntaxException {
        web = new WebImpl();
        this.context = context;
        System.out.println("Starting graphql....");
        try {
            String authFilter = String.format("(AuthIdentifier=%s)", AUTH_IDENTIFIER);
            String coordinatorFilter = "(objectClass=io.stargate.coordinator.Coordinator)";
            context.addServiceListener(this, String.format("(|%s%s)", coordinatorFilter, authFilter));
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
                    System.out.println("Setting authenticationService in GraphqlActivator");
                    this.web.setAuthenticationService((AuthenticationService)service);
                    break;
                }
            }
        }

        coordinatorReference = context.getServiceReference(Coordinator.class.getName());
        if (coordinatorReference != null) {
            System.out.println("Setting coordinator in GraphqlActivator");
            this.web.setCoordinator((Coordinator)context.getService(coordinatorReference));
        }

        if (this.web.getCoordinator() != null && this.web.getAuthenticationService() != null) {
            try {
                this.web.start();
                System.out.println("Started graphql....");
            } catch (Exception e) {
                System.out.println("Failed");
                e.printStackTrace();
            }
        }
    }

    @Override
    public void stop(BundleContext context) {
        if (coordinatorReference != null) {
            context.ungetService(coordinatorReference);
        }
        if (authenticationReference != null) {
            context.ungetService(authenticationReference);
        }
        try {
            web.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void serviceChanged(ServiceEvent serviceEvent) {
        int type = serviceEvent.getType();
        String[] objectClass = (String[]) serviceEvent.getServiceReference().getProperty("objectClass");
        switch (type) {
            case (ServiceEvent.REGISTERED):
                System.out.println("Service of type " + objectClass[0] + " registered.");
                Object service = context.getService(serviceEvent.getServiceReference());

                if (service instanceof Coordinator) {
                    System.out.println("Setting coordinator in GraphqlActivator");
                    this.web.setCoordinator((Coordinator) service);
                } else if (service instanceof AuthenticationService && serviceEvent.getServiceReference().getProperty("AuthIdentifier") != null
                        && serviceEvent.getServiceReference().getProperty("AuthIdentifier").equals(AUTH_IDENTIFIER)) {
                    System.out.println("Setting authenticationService in GraphqlActivator");
                    this.web.setAuthenticationService((AuthenticationService)service);
                }

                if (this.web.getCoordinator() != null && this.web.getAuthenticationService() != null) {
                    System.out.println("Started graphql...." + service);
                    try {
                        web.start();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                break;
            case (ServiceEvent.UNREGISTERING):
                try {
                    web.stop();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
        }
    }
}
