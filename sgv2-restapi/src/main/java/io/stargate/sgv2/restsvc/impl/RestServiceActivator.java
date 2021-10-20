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

import io.stargate.core.activator.BaseActivator;
import io.stargate.core.metrics.api.HttpMetricsTagProvider;
import io.stargate.core.metrics.api.Metrics;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Activator for the restapi bundle */
public class RestServiceActivator extends BaseActivator {

  public static final String MODULE_NAME = "restapi";
  private static final Logger logger = LoggerFactory.getLogger(RestServiceActivator.class);
  private final RestServiceStarter runner = new RestServiceStarter();
  private final ServicePointer<Metrics> metrics = ServicePointer.create(Metrics.class);
  private final ServicePointer<HttpMetricsTagProvider> httpTagProvider =
      ServicePointer.create(HttpMetricsTagProvider.class);

  public RestServiceActivator() {
    super("restapi", true);
  }

  @Override
  protected ServiceAndProperties createService() {
    runner.setMetrics(metrics.get());
    runner.setHttpMetricsTagProvider(httpTagProvider.get());
    try {
      runner.start();
    } catch (Exception e) {
      logger.error("Running RestServiceRunner Failed", e);
    }
    // Shouldn't we return something to avoid add logging for "service null"?
    return null;
  }

  @Override
  protected List<ServicePointer<?>> dependencies() {
    return Arrays.asList(metrics, httpTagProvider);
  }
}
