package io.stargate.config.store.k8s.config.map;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceEvent;
import org.osgi.framework.ServiceListener;

public class ConfigStoreActivator implements BundleActivator, ServiceListener {
  @Override
  public void start(BundleContext context) throws Exception {}

  @Override
  public void stop(BundleContext context) throws Exception {}

  @Override
  public void serviceChanged(ServiceEvent event) {}
}
