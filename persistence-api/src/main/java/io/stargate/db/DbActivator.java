package io.stargate.db;

import com.codahale.metrics.MetricRegistry;
import io.stargate.db.cdc.CDCProducer;
import io.stargate.db.cdc.CDCService;
import io.stargate.db.cdc.CDCServiceImpl;
import org.osgi.framework.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbActivator implements BundleActivator, ServiceListener {
  private static final Logger logger = LoggerFactory.getLogger(DbActivator.class);
  public CDCService cdcService;

  @Override
  public void start(BundleContext context) {
    logger.info("Starting DbActivator...");
    // TODO: Use metrics registry from Metrics service
    MetricRegistry registry = new MetricRegistry();

    ServiceReference<CDCProducer> producerReference =
        context.getServiceReference(CDCProducer.class);
    if (producerReference != null) {
      logger.info("Creating CDC service");
      // TODO: Maybe
      cdcService = new CDCServiceImpl(context.getService(producerReference), registry);
    }

    // TODO: initialize CDC service
    // TODO: Do something with CDC Service
  }

  @Override
  public void stop(BundleContext context) {}

  @Override
  public void serviceChanged(ServiceEvent serviceEvent) {}
}
