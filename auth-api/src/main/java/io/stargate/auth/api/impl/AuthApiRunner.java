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
package io.stargate.auth.api.impl;

import io.stargate.auth.AuthenticationService;
import io.stargate.core.metrics.api.HttpMetricsTagProvider;
import io.stargate.core.metrics.api.Metrics;

public class AuthApiRunner {
  private final AuthenticationService authenticationService;
  private final Metrics metrics;
  private final HttpMetricsTagProvider httpMetricsTagProvider;

  public AuthApiRunner(
      AuthenticationService authenticationService,
      Metrics metrics,
      HttpMetricsTagProvider httpMetricsTagProvider) {
    this.authenticationService = authenticationService;
    this.metrics = metrics;
    this.httpMetricsTagProvider = httpMetricsTagProvider;
  }

  public void start() {
    AuthApiServer authApiServer =
        new AuthApiServer(this.authenticationService, this.metrics, this.httpMetricsTagProvider);
    authApiServer.run("server", "authapi-config.yaml");
  }
}
