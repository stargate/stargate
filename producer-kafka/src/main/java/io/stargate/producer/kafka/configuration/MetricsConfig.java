package io.stargate.producer.kafka.configuration;

import com.codahale.metrics.MetricRegistry;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;
import java.util.Optional;

public class MetricsConfig {
  public static final boolean INCLUDE_TAGS_DEFAULT = true;
  public static final String METRICS_NAME_DEFAULT = "producer";
  private final boolean metricsEnabled;
  private final Boolean includeTags;
  private final String metricsName;

  public MetricsConfig(boolean metricsEnabled, boolean includeTags, String metricsName) {
    this.metricsEnabled = metricsEnabled;
    this.includeTags = includeTags;
    this.metricsName = metricsName;
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  public static MetricsConfig create(
      boolean metricsEnabled, Optional<Boolean> metricsIncludeTags, Optional<String> metricsName) {
    return new MetricsConfig(
        metricsEnabled,
        metricsIncludeTags.orElse(INCLUDE_TAGS_DEFAULT),
        metricsName.orElse(METRICS_NAME_DEFAULT));
  }

  /** @return whether the kafka producer metrics should be exposed via {@link MetricRegistry}. */
  public boolean isMetricsEnabled() {
    return metricsEnabled;
  }

  /** @return whether the constructed metric name should include tags. */
  public boolean isIncludeTags() {
    return includeTags;
  }

  /**
   * @return the metric name that will be used as a prefix for all registered kafka producer
   *     metrics.
   */
  @NonNull
  public String getMetricsName() {
    return metricsName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    MetricsConfig that = (MetricsConfig) o;

    if (metricsEnabled != that.metricsEnabled) return false;
    if (!Objects.equals(includeTags, that.includeTags)) return false;
    return Objects.equals(metricsName, that.metricsName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(metricsEnabled, includeTags, metricsName);
  }
}
