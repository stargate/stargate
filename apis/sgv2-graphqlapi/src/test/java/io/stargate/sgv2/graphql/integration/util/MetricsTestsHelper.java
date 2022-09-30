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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.sgv2.graphql.integration.util;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collector;

public class MetricsTestsHelper {

  public static Double getMetricValue(
      String body,
      String metricName,
      Pattern regexpPattern,
      Collector<Double, ?, Double> aggregator) {
    return Arrays.stream(body.split("\n"))
        .filter(v -> v.contains(metricName))
        .filter(v -> regexpPattern.matcher(v).find())
        .map(
            v -> {
              Matcher matcher = regexpPattern.matcher(v);
              if (matcher.find()) {
                return matcher.group(2);
              }
              throw new IllegalArgumentException(
                  String.format("Value: %s does not contain the numeric value for metric", v));
            })
        .map(Double::parseDouble)
        .collect(aggregator);
  }
}
