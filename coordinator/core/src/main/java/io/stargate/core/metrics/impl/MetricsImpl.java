package io.stargate.core.metrics.impl;

import com.codahale.metrics.MetricRegistry;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.prometheus.metrics.instrumentation.dropwizard.DropwizardExports;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.stargate.core.metrics.StargateMetricConstants;
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
    // 1. Build the DropwizardExports using the builder API in client 1.x
    DropwizardExports dropwizardExports = new DropwizardExports(metricRegistry);

    // 2. Create the new PrometheusRegistry (replaces CollectorRegistry) and register exports
    PrometheusRegistry prometheusRegistry = new PrometheusRegistry();
    prometheusRegistry.register(dropwizardExports);

    // 3. Create the PrometheusMeterRegistry (from io.micrometer.prometheusmetrics)
    PrometheusMeterRegistry meterRegistry =
        new PrometheusMeterRegistry(PrometheusConfig.DEFAULT, prometheusRegistry, Clock.SYSTEM);

    MeterRegistryConfiguration.configure(meterRegistry);
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
  public Tags tagsForModule(String module) {
    Tag moduleTag =
        null != module
            ? Tag.of(StargateMetricConstants.MODULE_KEY, module)
            : StargateMetricConstants.TAG_MODULE_UNKNOWN;
    return Tags.of(moduleTag);
  }

  @Override
  public String scrape() {
    return prometheusMeterRegistry.scrape();
  }
}
