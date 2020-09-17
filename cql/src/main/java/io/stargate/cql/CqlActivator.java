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

import com.google.common.base.Strings;
import io.stargate.auth.AuthenticationService;
import io.stargate.cql.impl.CqlImpl;
import io.stargate.db.Persistence;

public class CqlActivator implements BundleActivator, ServiceListener {
    private static final Logger log = LoggerFactory.getLogger(CqlActivator.class);

    private BundleContext context;
    private final CqlImpl cql = new CqlImpl(makeConfig());
    private ServiceReference persistenceReference;
    private ServiceReference authenticationReference;

    static String AUTH_IDENTIFIER = System.getProperty("stargate.cql_auth_id");
    static String PERSISTENCE_IDENTIFIER = System.getProperty("stargate.persistence_id", "CassandraPersistence");

    private static Config makeConfig()
    {
        try
        {
            String listenAddress = System.getProperty("stargate.listen_address", InetAddress.getLocalHost().getHostAddress());
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
    public void start(BundleContext context) throws InvalidSyntaxException
    {
        this.context = context;
        log.info("Starting CQL....");
        synchronized (cql) {
            context.addServiceListener(this, String.format("(|(AuthIdentifier=%s)(Identifier=%s))", AUTH_IDENTIFIER, PERSISTENCE_IDENTIFIER));

            ServiceReference[] refs = context.getServiceReferences(AuthenticationService.class.getName(), null);
            if (refs != null) {
                for (ServiceReference ref : refs) {
                    // Get the service object.
                    Object service = context.getService(ref);
                    if (service instanceof AuthenticationService && ref.getProperty("AuthIdentifier") != null
                            && ref.getProperty("AuthIdentifier").equals(AUTH_IDENTIFIER)) {
                        log.info("Setting authenticationService in CqlActivator");
                        authenticationReference = ref;
                        break;
                    }
                }
            }

            persistenceReference = context.getServiceReference(Persistence.class.getName());
            if (persistenceReference != null)
                log.info("Setting persistence in CqlActivator");

            maybeStartService();
        }
    }

    @Override
    public void stop(BundleContext context) {
        context.ungetService(persistenceReference);
        if (authenticationReference != null)
            context.ungetService(authenticationReference);
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
                        log.info("Setting persistence in CqlActivator");
                        persistenceReference = serviceEvent.getServiceReference();
                    } else if (service instanceof AuthenticationService && serviceEvent.getServiceReference().getProperty("AuthIdentifier") != null
                            && serviceEvent.getServiceReference().getProperty("AuthIdentifier").equals(AUTH_IDENTIFIER)) {
                        log.info("Setting authenticationService in RestApiActivator");
                        authenticationReference = serviceEvent.getServiceReference();
                    }

                    maybeStartService();
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

    public void maybeStartService()
    {
        if (persistenceReference != null && (!requiresAuthentication() || authenticationReference != null)) {
            Object persistenceService = context.getService(persistenceReference);

            Object authenticationService = null;
            if (authenticationReference != null)
                authenticationService = context.getService(authenticationReference);

            if (persistenceService != null) {
                this.cql.start((Persistence) persistenceService, (AuthenticationService)authenticationService);
                log.info("Started CQL....");
            }
        }
    }

    public boolean requiresAuthentication()
    {
        return !Strings.isNullOrEmpty(AUTH_IDENTIFIER);
    }

}
