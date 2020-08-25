package io.stargate.coordinator;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceEvent;
import org.osgi.framework.ServiceListener;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;

import io.stargate.db.Persistence;
import io.stargate.filterchain.FilterChain;

public class CoordinatorActivator implements BundleActivator, ServiceListener {

    private ServiceRegistration<?> registration;
    private BundleContext context;
    private final Coordinator coordinator = new Coordinator();
    private ServiceReference persistenceReference;
    private ServiceReference readFilterChainReference;
    private ServiceReference writeFilterChainReference;

    static String PERSISTENCE_IDENTIFIER = "CassandraPersistence";

    static {
        String persistence = System.getProperty("stargate.persistence_id");
        if (persistence != null) {
            PERSISTENCE_IDENTIFIER = persistence;
        }
    }

    public void start(BundleContext context) {
        this.context = context;
        synchronized (coordinator) {
            try {
                context.addServiceListener(this, String.format("(|(Identifier=%s)(Identifier=ReadFilterChain)(Identifier=WriteFilterChain))", PERSISTENCE_IDENTIFIER));
            } catch (InvalidSyntaxException ise) {
                throw new RuntimeException(ise);
            }

            persistenceReference = context.getServiceReference(Persistence.class.getName());
            if (persistenceReference != null && persistenceReference.getProperty("Identifier").equals(PERSISTENCE_IDENTIFIER)) {
                this.coordinator.setPersistence((Persistence) context.getService(persistenceReference));
            }

            if (readFilterChainReference != null && readFilterChainReference.getProperty("Identifier").equals("ReadFilterChain")) {
                System.out.println("Setting readFilterChain in CoordinatorActivator");
                this.coordinator.setReadFilterChain((FilterChain) context.getService(readFilterChainReference));
            }

            if (writeFilterChainReference != null && writeFilterChainReference.getProperty("Identifier").equals("WriteFilterChain")) {
                System.out.println("Setting writeFilterChain in CoordinatorActivator");
                this.coordinator.setWriteFilterChain((FilterChain) context.getService(writeFilterChainReference));
            }

            if (persistenceReference != null) {
                registration = context.registerService(Coordinator.class.getName(), this.coordinator, null);
            }
        }
    }

    public void stop(BundleContext context) {
        if (persistenceReference != null) {
            context.ungetService(persistenceReference);
        }

        if (readFilterChainReference != null) {
            context.ungetService(readFilterChainReference);
        }

        if (writeFilterChainReference != null) {
            context.ungetService(writeFilterChainReference);
        }
    }

    @Override
    public void serviceChanged(ServiceEvent serviceEvent) {
        int type = serviceEvent.getType();
        String[] objectClass = (String[]) serviceEvent.getServiceReference().getProperty("objectClass");
        synchronized (coordinator) {
            switch (type) {
                case (ServiceEvent.REGISTERED):
                    System.out.println("Service of type " + objectClass[0] + " registered.");
                    Object service = context.getService(serviceEvent.getServiceReference());
                    if (service instanceof Persistence && serviceEvent.getServiceReference().getProperty("Identifier") != null
                            && serviceEvent.getServiceReference().getProperty("Identifier").equals(PERSISTENCE_IDENTIFIER)) {
                        System.out.println("Setting persistence in CoordinatorActivator");
                        this.persistenceReference = serviceEvent.getServiceReference();
                        this.coordinator.setPersistence((Persistence) service);
                    } else {
                        System.out.println("Setting filterChain in CoordinatorActivator");
                        System.out.println("identifier " + serviceEvent.getServiceReference().getProperty("Identifier"));
                        if (serviceEvent.getServiceReference().getProperty("Identifier") != null && serviceEvent.getServiceReference().getProperty("Identifier").equals("ReadFilterChain")) {
                            System.out.println("Setting readFilterChain in CoordinatorActivator");
                            this.readFilterChainReference = serviceEvent.getServiceReference();
                            this.coordinator.setReadFilterChain((FilterChain) service);
                        }

                        if (serviceEvent.getServiceReference().getProperty("Identifier") != null && serviceEvent.getServiceReference().getProperty("Identifier").equals("WriteFilterChain")) {
                            System.out.println("Setting writeFilterChain in CoordinatorActivator");
                            this.writeFilterChainReference = serviceEvent.getServiceReference();
                            this.coordinator.setWriteFilterChain((FilterChain) service);
                        }
                    }

                    if (registration == null && this.coordinator.getPersistence() != null) {
                        registration = context.registerService(Coordinator.class.getName(), this.coordinator, null);
                    }
                    break;
                case (ServiceEvent.UNREGISTERING):
                    System.out.println("Service of type " + objectClass[0] + " unregistered.");
                    context.ungetService(serviceEvent.getServiceReference());
                    break;
                case (ServiceEvent.MODIFIED):
                    // TODO: [doug] 2020-06-15, Mon, 12:58 do something here...
                    System.out.println("Service of type " + objectClass[0] + " modified.");
                    break;
                default:
                    break;
            }
        }
    }
}
