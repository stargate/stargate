package io.stargate.db;

import io.stargate.core.CoreActivator;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.core.metrics.impl.MetricsImpl;
import io.stargate.db.datastore.DataStoreFactory;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceEvent;
import org.osgi.framework.ServiceListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbActivator implements BundleActivator, ServiceListener {
  private static final Logger logger = LoggerFactory.getLogger(CoreActivator.class);

  @Override
  public void start(BundleContext context) {
    logger.info("Registering DB services...");

    context.registerService(DataStoreFactory.class, new DataStoreFactory(), null);
  }

  @Override
  public void stop(BundleContext bundleContext) throws Exception {}

  @Override
  public void serviceChanged(ServiceEvent serviceEvent) {}
}

