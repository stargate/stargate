package io.stargate.health;

import io.stargate.health.metrics.api.Metrics;
import io.stargate.health.metrics.impl.MetricsImpl;
import net.jcip.annotations.GuardedBy;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceEvent;
import org.osgi.framework.ServiceListener;
import org.osgi.framework.ServiceReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HealthCheckerActivator implements BundleActivator, ServiceListener {
  private static final Logger log = LoggerFactory.getLogger(HealthCheckerActivator.class);

  private BundleContext context;

  @GuardedBy("this")
  private WebImpl web = new WebImpl();

  @Override
  public synchronized void start(BundleContext context) {
    this.context = context;
    log.info("Starting healthchecker....");

    Metrics metrics = new MetricsImpl();
    context.registerService(Metrics.class, metrics, null);
    web.setContext(context);
    web.setMetrics(metrics);

    try {
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
