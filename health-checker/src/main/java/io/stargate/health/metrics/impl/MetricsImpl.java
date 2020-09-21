package io.stargate.health.metrics.impl;

import com.codahale.metrics.MetricRegistry;
import io.stargate.health.metrics.api.Metrics;

public class MetricsImpl implements Metrics {

  private final MetricRegistry registry;

  public MetricsImpl() {
    registry = new MetricRegistry();
  }

  @Override
  public MetricRegistry getRegistry() {
    return registry;
  }

  @Override
  public MetricRegistry getRegistry(String prefix) {
    return new PrefixingMetricRegistry(registry, prefix);
  }
}
