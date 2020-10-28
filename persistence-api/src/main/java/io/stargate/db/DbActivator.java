package io.stargate.db;

import io.stargate.core.BundleUtils;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.db.cdc.CDCProducer;
import io.stargate.db.cdc.CDCService;
import io.stargate.db.cdc.CDCServiceImpl;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceEvent;
import org.osgi.framework.ServiceListener;
import org.osgi.framework.ServiceReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbActivator implements BundleActivator, ServiceListener {
  private static final Logger logger = LoggerFactory.getLogger(DbActivator.class);
  public CDCService cdcService;
  private boolean started;
  private BundleContext context;
  private CDCProducer producer;
  private Metrics metrics;

  @Override
  public synchronized void start(BundleContext context) throws InvalidSyntaxException {
    this.context = context;
    logger.info("Starting io.stargate.db.DbActivator...");

    context.addServiceListener(
        this,
        String.format(
            "(|(objectClass=%s)(objectClass=%s))",
            CDCProducer.class.getName(), Metrics.class.getName()));

    ServiceReference<CDCProducer> producerReference =
        context.getServiceReference(CDCProducer.class);

    ServiceReference<Metrics> metricsReference = context.getServiceReference(Metrics.class);

    if (producerReference != null) {
      producer = context.getService(producerReference);
    }

    if (metricsReference != null) {
      metrics = context.getService(metricsReference);
    }

    startIfReady();
  }

  private synchronized void startIfReady() {
    if (started || producer == null || metrics == null) {
      return;
    }

    started = true;
    logger.info("Creating CDC service...");
    cdcService = new CDCServiceImpl(producer, metrics);

    // TODO: initialize CDC service
    // TODO: Do something with CDC Service
  }

  @Override
  public synchronized void stop(BundleContext context) {
    // No way to stop CDC or other services yet
  }

  @Override
  public synchronized void serviceChanged(ServiceEvent serviceEvent) {
    Metrics metrics = BundleUtils.getRegisteredService(context, serviceEvent, Metrics.class);
    CDCProducer producer =
        BundleUtils.getRegisteredService(context, serviceEvent, CDCProducer.class);

    if (metrics != null) {
      this.metrics = metrics;
    }

    if (producer != null) {
      this.producer = producer;
    }

    startIfReady();
  }
}
