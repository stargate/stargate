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
package io.stargate.sgv2.restsvc.impl;

import io.stargate.core.metrics.api.HttpMetricsTagProvider;
import io.stargate.core.metrics.api.Metrics;

public class RestServiceRunner {
  private Metrics metrics;
  private HttpMetricsTagProvider httpMetricsTagProvider;

  public void setMetrics(Metrics metrics) {
    this.metrics = metrics;
  }

  public void setHttpMetricsTagProvider(HttpMetricsTagProvider httpMetricsTagProvider) {
    this.httpMetricsTagProvider = httpMetricsTagProvider;
  }

  public void start() throws Exception {
    RestServiceServer server = new RestServiceServer(this.metrics, this.httpMetricsTagProvider);
    server.run("server", "config.yaml");
  }
}
