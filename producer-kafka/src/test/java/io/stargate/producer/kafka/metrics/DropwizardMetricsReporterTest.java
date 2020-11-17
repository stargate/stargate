package io.stargate.producer.kafka.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DropwizardMetricsReporterTest {

  private MetricRegistry registry;
  private DropwizardMetricsReporter metricsReporter;

  @BeforeEach
  public void setUp() {
    this.registry = new MetricRegistry();
    this.metricsReporter = new DropwizardMetricsReporter(registry);
  }

  @Test
  public void shouldInitializeIncludeTagsSettingWhenPassingInProperConfigs() {

    metricsReporter.configure(
        ImmutableMap.of(
            DropwizardMetricsReporter.SHOULD_INCLUDE_TAGS_CONFIG,
            Boolean.TRUE.toString(),
            DropwizardMetricsReporter.METRICS_NAME_CONFIG,
            "test"));

    assertThat(metricsReporter.isIncludeTags()).isTrue();
  }

  @Test
  public void shouldDefaultIncludeTagsSettingToFalseWhenNotPassingConfigs() {
    metricsReporter.configure(
        ImmutableMap.of(DropwizardMetricsReporter.METRICS_NAME_CONFIG, "test"));

    assertThat(metricsReporter.isIncludeTags()).isFalse();
  }

  @Test
  public void initShouldSuccessfullyCreateAllMetricsWithoutTags() {
    final Metrics metrics = new Metrics();
    final MetricName metricName =
        metrics.metricName("metric1", "group1", ImmutableMap.of("key1", "value1"));
    metrics.addMetric(metricName, (config, time) -> 1.0);
    final MetricName metricName2 = metrics.metricName("metric2", "group2", ImmutableMap.of());
    metrics.addMetric(metricName2, (config, time) -> 1.0);
    final KafkaMetric kafkaMetric = metrics.metric(metricName);
    final KafkaMetric kafkaMetric2 = metrics.metric(metricName2);

    metricsReporter.configure(
        ImmutableMap.of(DropwizardMetricsReporter.METRICS_NAME_CONFIG, "test"));

    metricsReporter.init(ImmutableList.of(kafkaMetric, kafkaMetric2));

    assertThat(registry.getGauges().get(metricsReporter.createMetricName(metricName)).getValue())
        .isEqualTo(1.0);
    assertThat(registry.getGauges().get(metricsReporter.createMetricName(metricName2)).getValue())
        .isEqualTo(1.0);
  }

  @Test
  public void initShouldSuccessfullyCreateAllMetricsIncludingTags() {
    final Metrics metrics = new Metrics();
    final MetricName metricName =
        metrics.metricName("metric1", "group1", ImmutableMap.of("key1", "value1"));
    metrics.addMetric(metricName, (config, time) -> 1.0);
    final MetricName metricName2 = metrics.metricName("metric2", "group2", ImmutableMap.of());
    metrics.addMetric(metricName2, (config, time) -> 1.0);
    final KafkaMetric kafkaMetric = metrics.metric(metricName);
    final KafkaMetric kafkaMetric2 = metrics.metric(metricName2);

    metricsReporter.configure(
        ImmutableMap.of(
            DropwizardMetricsReporter.SHOULD_INCLUDE_TAGS_CONFIG,
            Boolean.TRUE.toString(),
            DropwizardMetricsReporter.METRICS_NAME_CONFIG,
            "test"));

    metricsReporter.init(ImmutableList.of(kafkaMetric, kafkaMetric2));

    metricsReporter
        .createMetricNamesWithTags(metricName)
        .forEach(name -> assertThat(registry.getGauges().get(name).getValue()).isEqualTo((1.0)));
  }

  @Test
  public void createMetricNameShouldResultInExpectedFormat() {
    final MetricName metricName =
        new MetricName(
            "name",
            "group",
            "description",
            ImmutableMap.of("tagKey1", "tagValue1", "tagKey2", "tagValue2"));

    metricsReporter.configure(
        ImmutableMap.of(DropwizardMetricsReporter.METRICS_NAME_CONFIG, "test"));

    assertThat(metricsReporter.createMetricName(metricName)).isEqualTo("test.group.name");
  }

  @Test
  public void createMetricNamesWithTagsShouldResultInExpectedFormat() {
    final MetricName metricName =
        new MetricName(
            "name",
            "group",
            "description",
            ImmutableMap.of("tagKey1", "tagValue1", "tagKey2", "tagValue2"));

    metricsReporter.configure(
        ImmutableMap.of(DropwizardMetricsReporter.METRICS_NAME_CONFIG, "test"));

    assertThat(metricsReporter.createMetricNamesWithTags(metricName))
        .contains("test.group.name.tagKey1.tagValue1", "test.group.name.tagKey2.tagValue2");
  }

  @Test
  public void createMetricNamesWithTagsForUntaggedMetricNameShouldResultInExpectedFormat() {
    final MetricName metricName =
        new MetricName("metric2", "group2", "description", ImmutableMap.of());

    metricsReporter.configure(
        ImmutableMap.of(DropwizardMetricsReporter.METRICS_NAME_CONFIG, "test"));

    assertThat(metricsReporter.createMetricNamesWithTags(metricName).get(0))
        .isEqualTo("test.group2.metric2");
  }

  @Test
  public void metricChangeShouldSuccessfullyUpdateExistingMetricWithMeasurableMetric() {
    final Metrics metrics = new Metrics();
    final MetricName metricName =
        metrics.metricName("metric1", "group1", ImmutableMap.of("key1", "value1"));
    metrics.addMetric(metricName, (config, time) -> 1.0);
    final KafkaMetric kafkaMetric = metrics.metric(metricName);

    metricsReporter.configure(
        ImmutableMap.of(DropwizardMetricsReporter.METRICS_NAME_CONFIG, "test"));

    metricsReporter.init(ImmutableList.of(kafkaMetric));

    // remove and add an updated metric
    metrics.removeMetric(metricName);
    metrics.addMetric(metricName, (config, time) -> 2.0);

    final KafkaMetric updatedMetric = metrics.metric(metricName);

    metricsReporter.metricChange(updatedMetric);

    assertThat(registry.getGauges().get(metricsReporter.createMetricName(metricName)).getValue())
        .isEqualTo(2.0);
  }

  @Test
  public void metricChangeShouldSuccessfullyUpdateExistingMetricWithGaugeMetric() {
    final Metrics metrics = new Metrics();
    final MetricName metricName =
        metrics.metricName("metric1", "group1", ImmutableMap.of("key1", "value1"));
    metrics.addMetric(metricName, (org.apache.kafka.common.metrics.Gauge) (config, time) -> 1.0);

    final KafkaMetric kafkaMetric = metrics.metric(metricName);

    metricsReporter.configure(
        ImmutableMap.of(DropwizardMetricsReporter.METRICS_NAME_CONFIG, "test"));

    metricsReporter.init(ImmutableList.of(kafkaMetric));

    // remove and add an updated metric
    metrics.removeMetric(metricName);
    metrics.addMetric(metricName, (org.apache.kafka.common.metrics.Gauge) (config, time) -> 2.0);

    final KafkaMetric updatedMetric = metrics.metric(metricName);

    metricsReporter.metricChange(updatedMetric);

    assertThat(registry.getGauges().get(metricsReporter.createMetricName(metricName)).getValue())
        .isEqualTo(2.0);
  }

  @Test
  public void metricChangeShouldSuccessfullyUpdateExistingMetricsForTags() {
    final Metrics metrics = new Metrics();
    final MetricName metricName =
        metrics.metricName("metric1", "group1", ImmutableMap.of("key1", "value1"));
    metrics.addMetric(metricName, (config, time) -> 1.0);
    final KafkaMetric kafkaMetric = metrics.metric(metricName);

    metricsReporter.configure(
        ImmutableMap.of(
            DropwizardMetricsReporter.SHOULD_INCLUDE_TAGS_CONFIG,
            Boolean.TRUE.toString(),
            DropwizardMetricsReporter.METRICS_NAME_CONFIG,
            "test"));

    metricsReporter.init(ImmutableList.of(kafkaMetric));

    metrics.removeMetric(metricName);
    metrics.addMetric(metricName, (config, time) -> 2.0);
    final KafkaMetric updatedMetric = metrics.metric(metricName);

    metricsReporter.metricChange(updatedMetric);

    metricsReporter
        .createMetricNamesWithTags(metricName)
        .forEach(name -> assertThat(registry.getGauges().get(name).getValue()).isEqualTo(2.0));
  }

  @Test
  public void metricChangeShouldSuccessfullyAddMetricThatDidNotPreviouslyExist() {
    final Metrics metrics = new Metrics();
    final MetricName metricName =
        metrics.metricName("metric1", "group1", ImmutableMap.of("key1", "value1"));
    metrics.addMetric(metricName, (config, time) -> 1.0);
    final KafkaMetric kafkaMetric = metrics.metric(metricName);

    metricsReporter.configure(
        ImmutableMap.of(DropwizardMetricsReporter.METRICS_NAME_CONFIG, "test"));

    metricsReporter.init(ImmutableList.of());

    metricsReporter.metricChange(kafkaMetric);

    assertThat(registry.getGauges().get(metricsReporter.createMetricName(metricName)).getValue())
        .isEqualTo(1.0);
  }

  @Test
  public void metricChangeShouldSuccessfullyAddTaggedMetricsThatDidNotPreviouslyExist() {
    final Metrics metrics = new Metrics();
    final MetricName metricName =
        metrics.metricName("metric1", "group1", ImmutableMap.of("key1", "value1"));
    metrics.addMetric(metricName, (config, time) -> 1.0);
    final KafkaMetric kafkaMetric = metrics.metric(metricName);

    metricsReporter.configure(
        ImmutableMap.of(
            DropwizardMetricsReporter.SHOULD_INCLUDE_TAGS_CONFIG,
            Boolean.TRUE.toString(),
            DropwizardMetricsReporter.METRICS_NAME_CONFIG,
            "test"));

    metricsReporter.init(ImmutableList.of());

    metricsReporter.metricChange(kafkaMetric);
    metricsReporter
        .createMetricNamesWithTags(metricName)
        .forEach(name -> assertThat(registry.getGauges().get(name).getValue()).isEqualTo(1.0));
  }

  @Test
  public void metricRemovalShouldResultInMetricRegistryReflectingRemovalWhenMetricExists() {
    final Metrics metrics = new Metrics();
    final MetricName metricName =
        metrics.metricName("metric1", "group1", ImmutableMap.of("key1", "value1"));
    metrics.addMetric(metricName, (config, time) -> 1.0);
    final KafkaMetric kafkaMetric = metrics.metric(metricName);

    metricsReporter.configure(
        ImmutableMap.of(DropwizardMetricsReporter.METRICS_NAME_CONFIG, "test"));

    metricsReporter.init(ImmutableList.of(kafkaMetric));

    metricsReporter.metricRemoval(kafkaMetric);

    assertThat(registry.getGauges().containsKey(metricsReporter.createMetricName(metricName)))
        .isFalse();
  }

  @Test
  public void
      metricRemovalShouldResultInMetricRegistryReflectingRemovalWhenMetricExistsForTaggedMetric() {
    final Metrics metrics = new Metrics();
    final MetricName metricName =
        metrics.metricName("metric1", "group1", ImmutableMap.of("key1", "value1"));
    metrics.addMetric(metricName, (config, time) -> 1.0);
    final KafkaMetric kafkaMetric = metrics.metric(metricName);

    metricsReporter.configure(
        ImmutableMap.of(
            DropwizardMetricsReporter.SHOULD_INCLUDE_TAGS_CONFIG,
            Boolean.TRUE.toString(),
            DropwizardMetricsReporter.METRICS_NAME_CONFIG,
            "test"));

    metricsReporter.init(ImmutableList.of(kafkaMetric));

    metricsReporter.metricRemoval(kafkaMetric);

    metricsReporter
        .createMetricNamesWithTags(metricName)
        .forEach(name -> assertThat(registry.getGauges().containsKey(name)).isFalse());
  }

  @Test
  public void metricRemovalShouldResultInANoopWhenNoMetricExists() {
    final Metrics metrics = new Metrics();
    final MetricName metricName =
        metrics.metricName("metric1", "group1", ImmutableMap.of("key1", "value1"));
    metrics.addMetric(metricName, (config, time) -> 1.0);
    final KafkaMetric kafkaMetric = metrics.metric(metricName);

    metricsReporter.configure(
        ImmutableMap.of(DropwizardMetricsReporter.METRICS_NAME_CONFIG, "test"));

    metricsReporter.metricRemoval(kafkaMetric);

    assertThat(registry.getGauges().containsKey(metricsReporter.createMetricName(metricName)))
        .isFalse();
  }

  @Test
  public void closeShouldRemoveAllRegisteredKafkaMetrics() {
    final Metrics metrics = new Metrics();
    final MetricName metricName =
        metrics.metricName("metric1", "group1", ImmutableMap.of("key1", "value1"));
    metrics.addMetric(metricName, (config, time) -> 1.0);
    final MetricName metricName2 = metrics.metricName("metric2", "group2", ImmutableMap.of());
    metrics.addMetric(metricName2, (config, time) -> 1.0);
    final KafkaMetric kafkaMetric = metrics.metric(metricName);
    final KafkaMetric kafkaMetric2 = metrics.metric(metricName2);

    metricsReporter.configure(
        ImmutableMap.of(
            DropwizardMetricsReporter.SHOULD_INCLUDE_TAGS_CONFIG,
            Boolean.TRUE.toString(),
            DropwizardMetricsReporter.METRICS_NAME_CONFIG,
            "test"));

    metricsReporter.init(ImmutableList.of(kafkaMetric, kafkaMetric2));

    metricsReporter.close();

    assertThat(registry.getGauges().isEmpty()).isTrue();
  }

  @Test
  public void closeShouldRemoveOnlyKafkaMetrics() {
    final Metrics metrics = new Metrics();
    final MetricName metricName =
        metrics.metricName("metric1", "group1", ImmutableMap.of("key1", "value1"));
    metrics.addMetric(metricName, (config, time) -> 1.0);
    final KafkaMetric kafkaMetric = metrics.metric(metricName);

    registry.register("test", (Gauge<Object>) Object::new);

    metricsReporter.configure(
        ImmutableMap.of(
            DropwizardMetricsReporter.SHOULD_INCLUDE_TAGS_CONFIG,
            Boolean.TRUE.toString(),
            DropwizardMetricsReporter.METRICS_NAME_CONFIG,
            "test"));

    metricsReporter.init(ImmutableList.of(kafkaMetric));

    metrics.close();

    assertThat(registry.getGauges().containsKey("test")).isTrue();
  }

  @Test
  public void closeShouldResultInANoopWhenNoMetricsAreRegistered() {
    final String metricName = "other-metric";
    registry.register(metricName, (Gauge<String>) this::toString);

    metricsReporter.configure(
        ImmutableMap.of(DropwizardMetricsReporter.METRICS_NAME_CONFIG, "test"));

    metricsReporter.close();

    assertThat(registry.getGauges().containsKey(metricName)).isTrue();
  }
}
