package io.stargate.core;

import com.codahale.metrics.health.HealthCheckRegistry;
import io.stargate.core.activator.BaseActivator;
import io.stargate.core.metrics.api.HttpMetricsTagProvider;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.core.metrics.api.MetricsScraper;
import io.stargate.core.metrics.impl.MetricsImpl;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CoreActivator extends BaseActivator {

  /**
   * Id if the {@link io.stargate.core.metrics.api.HttpMetricsTagProvider}. If not set, this
   * activator will register a default impl.
   */
  private static final String HTTP_TAG_PROVIDER_ID =
      System.getProperty("stargate.metrics.http_tag_provider.id");

  public CoreActivator() {
    super("core services");
  }

  @Override
  protected List<ServiceAndProperties> createServices() {
    MetricsImpl metricsImpl = new MetricsImpl();

    List<ServiceAndProperties> services = new ArrayList<>();
    services.add(new ServiceAndProperties(metricsImpl, Metrics.class, null));
    services.add(new ServiceAndProperties(metricsImpl, MetricsScraper.class, null));
    services.add(
        new ServiceAndProperties(new HealthCheckRegistry(), HealthCheckRegistry.class, null));

    // register default http tag provider if we are not using any special one
    if (null == HTTP_TAG_PROVIDER_ID) {
      services.add(
          new ServiceAndProperties(
              HttpMetricsTagProvider.DEFAULT, HttpMetricsTagProvider.class, null));
    }

    return services;
  }

  @Override
  protected List<ServicePointer<?>> dependencies() {
    return Collections.emptyList();
  }
}
