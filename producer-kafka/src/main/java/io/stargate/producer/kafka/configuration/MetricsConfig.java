package io.stargate.producer.kafka.configuration;

import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Objects;

public class MetricsConfig {
  private final boolean metricsEnabled;
  private final Boolean includeTags;
  private final String metricsName;

  public MetricsConfig(
      boolean metricsEnabled, @Nullable Boolean includeTags, @Nullable String metricsName) {
    this.metricsEnabled = metricsEnabled;
    this.includeTags = includeTags;
    this.metricsName = metricsName;
  }

  public boolean isMetricsEnabled() {
    return metricsEnabled;
  }

  @Nullable
  public Boolean isIncludeTags() {
    return includeTags;
  }

  @Nullable
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
