package io.stargate.core.metrics.impl;

import com.codahale.metrics.MetricRegistry;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.core.metrics.api.MetricsScraper;

public class MetricsImpl implements Metrics, MetricsScraper {

  private final MetricRegistry registry;

  private final PrometheusMeterRegistry prometheusMeterRegistry;

  public MetricsImpl() {
    registry = new MetricRegistry();
    prometheusMeterRegistry = initPrometheusMeterRegistry(registry);
  }

  private PrometheusMeterRegistry initPrometheusMeterRegistry(MetricRegistry metricRegistry) {
    // note that we are adding the dropwizard exports to the CollectorRegistry that we will pass to
    // the meter registry
    DropwizardExports dropwizardExports = new DropwizardExports(metricRegistry);
    CollectorRegistry collectorRegistry = new CollectorRegistry();
    collectorRegistry.register(dropwizardExports);

    PrometheusMeterRegistry meterRegistry =
        new PrometheusMeterRegistry(PrometheusConfig.DEFAULT, collectorRegistry, Clock.SYSTEM);
    return meterRegistry;
  }

  @Override
  public MetricRegistry getRegistry() {
    return registry;
  }

  @Override
  public MetricRegistry getRegistry(String prefix) {
    return new PrefixingMetricRegistry(registry, prefix);
  }

  @Override
  public MeterRegistry getMeterRegistry() {
    return prometheusMeterRegistry;
  }

  @Override
  public String scrape() {
    return prometheusMeterRegistry.scrape();
  }
}
