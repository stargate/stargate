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

import io.stargate.auth.AuthnzService;
import io.stargate.core.metrics.api.Metrics;

public class WebImpl {

  private AuthnzService authnzService;
  private Metrics metrics;

  public AuthnzService getAuthenticationService() {
    return authnzService;
  }

  public void setAuthenticationService(AuthnzService authnzService) {
    this.authnzService = authnzService;
  }

  public Metrics getMetrics() {
    return metrics;
  }

  public void setMetrics(Metrics metrics) {
    this.metrics = metrics;
  }

  public void start() {
    Server server = new Server(this.authnzService, this.metrics);
    server.run("server", "config.yaml");
  }
}
