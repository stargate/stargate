package io.stargate.producer.kafka.metrics;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DropwizardMetricsReporter implements MetricsReporter {
  private static final Logger log = LoggerFactory.getLogger(DropwizardMetricsReporter.class);

  public static final String SHOULD_INCLUDE_TAGS_CONFIG = "io.dropwizard.kafka.metrics.includeTags";
  public static final String METRICS_NAME_CONFIG = "io.dropwizard.kafka.metrics.name";

  private final MetricRegistry registry;
  private boolean includeTags = false;
  private String name;

  public DropwizardMetricsReporter() {
    this.registry = SharedMetricRegistries.getOrCreate("default");
  }

  public DropwizardMetricsReporter(final MetricRegistry registry) {
    this.registry = registry;
  }

  @Override
  public void init(final List<KafkaMetric> metrics) {
    if (includeTags) {
      metrics.stream()
          .filter(metric -> shouldRegister(metric::metricValue))
          .forEach(
              metric ->
                  createMetricNamesWithTags(metric.metricName())
                      .forEach(metricName -> tryRegister(metricName, metric::metricValue)));
    } else {
      metrics.stream()
          .filter(metric -> shouldRegister(metric::metricValue))
          .forEach(
              metric -> tryRegister(createMetricName(metric.metricName()), metric::metricValue));
    }
  }

  String createMetricName(final MetricName metricName) {
    return name(name, metricName.group(), metricName.name());
  }

  List<String> createMetricNamesWithTags(final MetricName metricName) {
    if (metricName.tags().isEmpty()) {
      return ImmutableList.of(createMetricName(metricName));
    }

    return metricName.tags().entrySet().stream()
        .map(
            entry ->
                name(name, metricName.group(), metricName.name(), entry.getKey(), entry.getValue()))
        .collect(Collectors.toList());
  }

  @Override
  public void metricChange(final KafkaMetric metric) {
    final Gauge<Object> metricToRegister = metric::metricValue;

    if (!shouldRegister(metricToRegister)) {
      return;
    }

    if (includeTags) {
      final List<String> metricNames = createMetricNamesWithTags(metric.metricName());

      metricNames.forEach(
          metricName -> {
            registry.remove(metricName);
            tryRegister(metricName, metricToRegister);
          });

    } else {
      final String metricName = createMetricName(metric.metricName());

      registry.remove(metricName);
      tryRegister(metricName, metricToRegister);
    }
  }

  @Override
  public void metricRemoval(final KafkaMetric metric) {
    if (includeTags) {
      createMetricNamesWithTags(metric.metricName()).forEach(registry::remove);
    } else {
      registry.remove(createMetricName(metric.metricName()));
    }
  }

  @Override
  public void close() {
    registry
        .getGauges((metricName, metric) -> metricName.startsWith(name))
        .forEach((metricName, metric) -> registry.remove(metricName));
  }

  @Override
  public void configure(final Map<String, ?> configs) {
    this.includeTags =
        Optional.ofNullable(configs.get(SHOULD_INCLUDE_TAGS_CONFIG))
            .filter(String.class::isInstance)
            .map(String.class::cast)
            .map(Boolean::parseBoolean)
            .orElse(false);

    this.name =
        Objects.requireNonNull(
            (String) configs.get(METRICS_NAME_CONFIG),
            "Metrics name required to configure DropwizardMetricsReporter");
  }

  boolean isIncludeTags() {
    return includeTags;
  }

  /**
   * Determines if this gauge should be registered at all, depending on the value it produces.
   *
   * @param gauge The gauge to possibly register
   * @return boolean indicating if this gauge should be registered
   */
  protected boolean shouldRegister(final Gauge<Object> gauge) {
    return gauge.getValue() instanceof Number;
  }

  private void tryRegister(final String metricName, final Gauge<Object> metricToRegister) {
    try {
      registry.register(metricName, metricToRegister);
    } catch (final IllegalArgumentException e) {
      // Swallow this, as the metric has already been registered.
      log.debug("Failed to register metric={}, as it is already registered", metricName);
    }
  }
}
