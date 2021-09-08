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

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.stargate.core.metrics.StargateMetricConstants;
import java.util.Arrays;
import java.util.stream.DoubleStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MeterRegistryConfiguration {

  private static final Logger logger = LoggerFactory.getLogger(MeterRegistryConfiguration.class);

  private static final String HTTP_PERCENTILES_PROPERTY =
      "stargate.metrics.http_server_requests_percentiles";

  private MeterRegistryConfiguration() {}

  public static void configure(MeterRegistry registry) {
    configureHttpPercentiles(registry.config());
  }

  public static void configure(MeterRegistry.Config config) {
    configureHttpPercentiles(config);
  }

  private static void configureHttpPercentiles(MeterRegistry.Config config) {
    String percentiles = System.getProperty(HTTP_PERCENTILES_PROPERTY);
    if (null != percentiles) {
      double[] finalPercentiles =
          Arrays.stream(percentiles.split(","))
              .map(String::trim)
              .flatMapToDouble(
                  p -> {
                    try {
                      double percentile = Double.parseDouble(p);
                      if (Double.isFinite(percentile) && percentile >= 0.0 && percentile <= 1.0) {
                        return DoubleStream.of(percentile);
                      }
                    } catch (NumberFormatException e) {
                    }
                    logger.warn(
                        "Value \"{}\" can not be used as the percentile for the {}, was expecting a double [0, 1].",
                        p,
                        HTTP_PERCENTILES_PROPERTY);
                    return DoubleStream.empty();
                  })
              .toArray();

      if (finalPercentiles.length > 0) {
        config.meterFilter(
            new PercentilesMeterFilter(
                finalPercentiles, StargateMetricConstants.METRIC_HTTP_SERVER_REQUESTS));
      }
    }
  }

  /** Simple {@link MeterFilter} that adds set of percentiles to a specific metric. */
  private static class PercentilesMeterFilter implements MeterFilter {

    private final double[] percentiles;
    private final String metricName;

    public PercentilesMeterFilter(double[] percentiles, String metricName) {
      this.percentiles = percentiles;
      this.metricName = metricName;
    }

    @Override
    public DistributionStatisticConfig configure(Meter.Id id, DistributionStatisticConfig config) {
      if (id.getName().startsWith(metricName)) {
        return DistributionStatisticConfig.builder().percentiles(percentiles).build().merge(config);
      } else {
        return config;
      }
    }
  }
}
