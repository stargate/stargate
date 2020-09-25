package io.stargate.health;

import io.stargate.core.metrics.api.Metrics;
import org.osgi.framework.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HealthCheckerActivator implements BundleActivator, ServiceListener {
  private static final Logger log = LoggerFactory.getLogger(HealthCheckerActivator.class);

  private BundleContext context;

  @Override
  public void start(BundleContext context) {
    this.context = context;
    log.info("Starting healthchecker....");

    ServiceReference<?> metricsReference = context.getServiceReference(Metrics.class.getName());

    if (metricsReference == null) {
      throw new RuntimeException("Metrics could not be loaded");
    }

    Metrics metrics = (Metrics) context.getService(metricsReference);

    try {
      WebImpl web = new WebImpl(this.context, metrics);
      web.start();
      log.info("Started healthchecker....");
    } catch (Exception e) {
      log.error("Failed", e);
    }
  }

  @Override
  public void stop(BundleContext context) {}

  @Override
  public void serviceChanged(ServiceEvent serviceEvent) {
    int type = serviceEvent.getType();
    String[] objectClass = (String[]) serviceEvent.getServiceReference().getProperty("objectClass");
    switch (type) {
      case (ServiceEvent.REGISTERED):
        log.info("Service of type " + objectClass[0] + " registered.");
        Object service = context.getService(serviceEvent.getServiceReference());

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
