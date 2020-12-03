package io.stargate.core;

import com.codahale.metrics.health.HealthCheckRegistry;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.core.metrics.impl.MetricsImpl;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceEvent;
import org.osgi.framework.ServiceListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CoreActivator implements BundleActivator, ServiceListener {
  private static final Logger logger = LoggerFactory.getLogger(CoreActivator.class);

  @Override
  public void start(BundleContext context) {
    logger.info("Registering core services...");

    Metrics metrics = new MetricsImpl();
    context.registerService(Metrics.class, metrics, null);
    context.registerService(HealthCheckRegistry.class, new HealthCheckRegistry(), null);
  }

  @Override
  public void stop(BundleContext bundleContext) throws Exception {}

  @Override
  public void serviceChanged(ServiceEvent serviceEvent) {}
}
