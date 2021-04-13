/*
 * Copyright The Stargate Authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.stargate.metrics.jersey;

import static io.stargate.core.metrics.impl.MetricsImpl.SERVER_HTTP_REQUESTS_METRIC_NAME;

import io.micrometer.jersey2.server.MetricsApplicationEventListener;
import io.stargate.core.metrics.api.HttpMetricsTagProvider;
import io.stargate.core.metrics.api.Metrics;

public class ResourceMetricsEventListener extends MetricsApplicationEventListener {

  public ResourceMetricsEventListener(
      Metrics metrics, HttpMetricsTagProvider httpMetricsTagProvider, String module) {
    super(
        metrics.getMeterRegistry(),
        new ResourceTagsProvider(httpMetricsTagProvider, module),
        SERVER_HTTP_REQUESTS_METRIC_NAME,
        true);
  }
}
