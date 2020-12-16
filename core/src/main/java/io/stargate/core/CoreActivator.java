package io.stargate.core;

import com.codahale.metrics.health.HealthCheckRegistry;
import io.stargate.core.activator.BaseActivator;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.core.metrics.impl.MetricsImpl;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class CoreActivator extends BaseActivator {

  public CoreActivator() {
    super("core services");
  }

  @Override
  protected List<ServiceAndProperties> createServices() {
    return Arrays.asList(
        new ServiceAndProperties(new MetricsImpl(), Metrics.class, null),
        new ServiceAndProperties(new HealthCheckRegistry(), HealthCheckRegistry.class, null));
  }

  @Override
  protected void stopService() {
    // no-op
  }

  @Override
  protected List<ServicePointer<?>> dependencies() {
    return Collections.emptyList();
  }
}
