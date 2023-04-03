/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.core.metrics.impl;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.stargate.core.metrics.StargateMetricConstants;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class MeterRegistryConfigurationTest {

  MeterRegistry meterRegistry;

  @BeforeEach
  public void initMeterRegistry() {
    meterRegistry = new SimpleMeterRegistry();
  }

  @Nested
  class Configure {

    @AfterEach
    public void cleanUpProperties() {
      System.clearProperty("stargate.metrics.http_server_requests_percentiles");
    }

    @Test
    public void happyPath() {
      System.setProperty(
          "stargate.metrics.http_server_requests_percentiles", "0.9, 0.95,0.99 ,0.999");

      MeterRegistryConfiguration.configure(meterRegistry);

      meterRegistry.summary(StargateMetricConstants.METRIC_HTTP_SERVER_REQUESTS).record(100d);
      HistogramSnapshot summary =
          meterRegistry
              .find(StargateMetricConstants.METRIC_HTTP_SERVER_REQUESTS)
              .summary()
              .takeSnapshot();

      assertThat(summary.percentileValues())
          .hasSize(4)
          .extracting(ValueAtPercentile::percentile)
          .containsOnly(0.9d, 0.95d, 0.99d, 0.999d);
    }

    @Test
    public void otherMetricsNotInfluenced() {
      System.setProperty(
          "stargate.metrics.http_server_requests_percentiles", "0.9,0.95,0.99,0.999");

      MeterRegistryConfiguration.configure(meterRegistry);

      meterRegistry.summary("some.other").record(100d);
      HistogramSnapshot summary = meterRegistry.find("some.other").summary().takeSnapshot();

      assertThat(summary.percentileValues()).isEmpty();
    }

    @Test
    public void propertyNotSet() {
      MeterRegistryConfiguration.configure(meterRegistry);

      meterRegistry.summary(StargateMetricConstants.METRIC_HTTP_SERVER_REQUESTS).record(100d);
      HistogramSnapshot summary =
          meterRegistry
              .find(StargateMetricConstants.METRIC_HTTP_SERVER_REQUESTS)
              .summary()
              .takeSnapshot();

      assertThat(summary.percentileValues()).isEmpty();
    }

    @Test
    public void wrongProperty() {
      System.setProperty("stargate.metrics.http_server_requests_percentiles", "something");

      MeterRegistryConfiguration.configure(meterRegistry);

      meterRegistry.summary(StargateMetricConstants.METRIC_HTTP_SERVER_REQUESTS).record(100d);
      HistogramSnapshot summary =
          meterRegistry
              .find(StargateMetricConstants.METRIC_HTTP_SERVER_REQUESTS)
              .summary()
              .takeSnapshot();

      assertThat(summary.percentileValues()).isEmpty();
    }

    @Test
    public void ignoreInvalid() {
      // only 0.5 is valid here
      System.setProperty(
          "stargate.metrics.http_server_requests_percentiles", "0.5,-0.5,,1.5,letters,NaN");

      MeterRegistryConfiguration.configure(meterRegistry);

      meterRegistry.summary(StargateMetricConstants.METRIC_HTTP_SERVER_REQUESTS).record(100d);
      HistogramSnapshot summary =
          meterRegistry
              .find(StargateMetricConstants.METRIC_HTTP_SERVER_REQUESTS)
              .summary()
              .takeSnapshot();

      assertThat(summary.percentileValues())
          .hasSize(1)
          .extracting(ValueAtPercentile::percentile)
          .containsOnly(0.5);
    }

    @Test
    public void edges() {
      System.setProperty("stargate.metrics.http_server_requests_percentiles", "0,1");

      MeterRegistryConfiguration.configure(meterRegistry);

      meterRegistry.summary(StargateMetricConstants.METRIC_HTTP_SERVER_REQUESTS).record(100d);
      HistogramSnapshot summary =
          meterRegistry
              .find(StargateMetricConstants.METRIC_HTTP_SERVER_REQUESTS)
              .summary()
              .takeSnapshot();

      assertThat(summary.percentileValues())
          .hasSize(2)
          .extracting(ValueAtPercentile::percentile)
          .containsOnly(0d, 1d);
    }
  }
}
