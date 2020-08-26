package io.stargate.auth.file;

import java.util.Hashtable;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

import io.stargate.auth.AuthenticationService;

public class AuthFileServiceActivator implements BundleActivator  {

    @Override
    public void start(BundleContext context) {
        Hashtable<String, String> props = new Hashtable<>();
        props.put("AuthIdentifier", "AuthFileService");
        AuthenticationService authFileService = new AuthFileService();
        ServiceRegistration<?> registration = context.registerService(AuthenticationService.class, authFileService, props);
    }

    @Override
    public void stop(BundleContext context) {
        // Do not need to unregister the service, because the OSGi framework will automatically do so
    }
}
