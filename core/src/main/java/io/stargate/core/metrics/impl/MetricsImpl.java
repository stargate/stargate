package io.stargate.core.metrics.impl;

import com.codahale.metrics.MetricRegistry;
import io.stargate.core.metrics.api.Metrics;

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
