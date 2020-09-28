package io.stargate.health;

import io.stargate.core.BundleUtils;
import io.stargate.core.metrics.api.Metrics;
import org.osgi.framework.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HealthCheckerActivator implements BundleActivator, ServiceListener {
  private static final Logger log = LoggerFactory.getLogger(HealthCheckerActivator.class);

  private BundleContext context;
  private boolean started;

  @Override
  public void start(BundleContext context) throws InvalidSyntaxException {
    this.context = context;

    ServiceReference<?> metricsReference = context.getServiceReference(Metrics.class.getName());

    if (metricsReference == null) {
      context.addServiceListener(this, String.format("(objectClass=%s)", Metrics.class.getName()));
      // Web will be started once the metrics service is registered
      return;
    }

    Metrics metrics = (Metrics) context.getService(metricsReference);
    startWeb(metrics);
  }

  private synchronized void startWeb(Metrics metrics) {
    if (started) {
      return;
    }

    started = true;
    log.info("Starting healthchecker....");
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
    Object service = BundleUtils.getRegisteredService(context, serviceEvent);

    if (service instanceof Metrics) {
      startWeb((Metrics) service);
    }
  }
}
