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
  private boolean started;
  private BundleContext context;

  @Override
  public void start(BundleContext context) throws InvalidSyntaxException {
    this.context = context;
    logger.info("Starting DbActivator...");

    context.addServiceListener(
        this, String.format("(objectClass=%s)", CDCProducer.class.getName()));

    ServiceReference<CDCProducer> producerReference =
        context.getServiceReference(CDCProducer.class);
    if (producerReference != null) {
      // TODO: Use metrics registry from Metrics service, after metrics circular dependency is
      // solved
      start(context.getService(producerReference), new MetricRegistry());
    }

    // TODO: initialize CDC service
    // TODO: Do something with CDC Service
  }

  private synchronized void start(CDCProducer producer, MetricRegistry metricRegistry) {
    if (started) {
      return;
    }

    started = true;
    logger.info("Creating CDC service...");
    cdcService = new CDCServiceImpl(producer, metricRegistry);
  }

  @Override
  public void stop(BundleContext context) {}

  @Override
  public void serviceChanged(ServiceEvent serviceEvent) {
    Object service = getRegisteredService(context, serviceEvent);

    if (service instanceof CDCProducer) {
      // TODO: Use metrics registry from Metrics service, after metrics circular dependency is
      // solved
      start((CDCProducer) service, new MetricRegistry());
    }
  }

  // TODO: Replace with core module method
  public static Object getRegisteredService(BundleContext context, ServiceEvent serviceEvent) {
    if (serviceEvent.getType() != ServiceEvent.REGISTERED) {
      return null;
    }

    String[] objectClass = (String[]) serviceEvent.getServiceReference().getProperty("objectClass");
    logger.info("Service of type " + objectClass[0] + " registered.");
    return context.getService(serviceEvent.getServiceReference());
  }
}
