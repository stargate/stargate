package io.stargate.health;

import io.stargate.core.activator.BaseActivator;
import io.stargate.core.metrics.api.Metrics;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.osgi.framework.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HealthCheckerActivator extends BaseActivator {
  private static final Logger log = LoggerFactory.getLogger(HealthCheckerActivator.class);

  public HealthCheckerActivator() {
    super("healthchecker");
  }

  ServicePointer<Metrics> metrics = ServicePointer.create(Metrics.class);

  @Override
  public synchronized void stop(BundleContext context) {}

  @Nullable
  @Override
  protected ServiceAndProperties createService() {
    log.info("Starting healthchecker....");
    try {
      WebImpl web = new WebImpl(context, metrics.get());
      web.start();
      log.info("Started healthchecker....");
    } catch (Exception e) {
      log.error("Failed", e);
    }
    return null;
  }

  @Override
  protected void stopService() {
    // no-op
  }

  @Override
  protected List<ServicePointer<?>> dependencies() {
    return Collections.singletonList(metrics);
  }
}
