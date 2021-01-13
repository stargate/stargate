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
package io.stargate.graphql.metrics;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

public class GraphMetrics {
  public static final GraphMetrics instance = new GraphMetrics();
  private volatile boolean initialized = false;
  private Meter graphQlOperations;

  public void markGraphQlOperation() {
    graphQlOperations.mark();
  }

  public synchronized void init(MetricRegistry metricRegistry) {
    if (initialized) return;

    graphQlOperations =
        metricRegistry.meter("org.apache.cassandra.metrics.Graph.GraphQlOperations");
  }
}
