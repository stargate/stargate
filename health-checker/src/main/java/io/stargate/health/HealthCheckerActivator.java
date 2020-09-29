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
  public synchronized void start(BundleContext context) throws InvalidSyntaxException {
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
  public synchronized void stop(BundleContext context) {}

  @Override
  public synchronized void serviceChanged(ServiceEvent serviceEvent) {
    Metrics metrics = BundleUtils.getRegisteredService(context, serviceEvent, Metrics.class);

    if (metrics != null) {
      startWeb(metrics);
    }
  }
}
