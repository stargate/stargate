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
package io.stargate.auth.api;

import io.stargate.auth.AuthenticationService;
import io.stargate.auth.api.impl.AuthApiRunner;
import io.stargate.core.activator.BaseActivator;
import io.stargate.core.metrics.api.HttpMetricsTagProvider;
import io.stargate.core.metrics.api.Metrics;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthApiActivator extends BaseActivator {

  public static final String MODULE_NAME = "authapi";

  private static final Logger log = LoggerFactory.getLogger(AuthApiActivator.class);

  private final ServicePointer<Metrics> metric = ServicePointer.create(Metrics.class);
  private final ServicePointer<HttpMetricsTagProvider> httpTagProvider =
      ServicePointer.create(HttpMetricsTagProvider.class);
  private final ServicePointer<AuthenticationService> authenticationService =
      ServicePointer.create(
          AuthenticationService.class,
          "AuthIdentifier",
          System.getProperty("stargate.auth_id", "AuthTableBasedService"));

  public AuthApiActivator() {
    super("authApiServer");
  }

  @Override
  protected ServiceAndProperties createService() {
    final AuthApiRunner runner =
        new AuthApiRunner(authenticationService.get(), metric.get(), httpTagProvider.get());
    try {
      runner.start();
    } catch (Exception e) {
      log.error("Starting AuthApiRunner Failed", e);
    }

    return null;
  }

  @Override
  protected List<ServicePointer<?>> dependencies() {
    return Arrays.asList(metric, httpTagProvider, authenticationService);
  }
}
