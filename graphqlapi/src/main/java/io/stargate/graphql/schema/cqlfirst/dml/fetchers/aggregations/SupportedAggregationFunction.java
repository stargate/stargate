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
package io.stargate.graphql.schema.cqlfirst.dml.fetchers.aggregations;

import java.util.Optional;

public enum SupportedAggregationFunction {
  COUNT("count"),
  AVG("avg"),
  MIN("min"),
  MAX("max"),
  SUM("sum");

  private final String name;

  SupportedAggregationFunction(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public static Optional<SupportedAggregationFunction> valueOfIgnoreCase(String functionName) {
    for (SupportedAggregationFunction f : values()) {
      if (f.name.equalsIgnoreCase(functionName)) {
        return Optional.of(f);
      }
    }
    return Optional.empty();
  }
}
