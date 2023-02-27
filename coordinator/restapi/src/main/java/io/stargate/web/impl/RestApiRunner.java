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
package io.stargate.web.impl;

import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthorizationService;
import io.stargate.core.metrics.api.HttpMetricsTagProvider;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.db.datastore.DataStoreFactory;

public class RestApiRunner {

  private AuthenticationService authenticationService;
  private AuthorizationService authorizationService;
  private Metrics metrics;
  private HttpMetricsTagProvider httpMetricsTagProvider;
  private DataStoreFactory dataStoreFactory;

  public void setAuthenticationService(AuthenticationService authenticationService) {
    this.authenticationService = authenticationService;
  }

  public void setAuthorizationService(AuthorizationService authorizationService) {
    this.authorizationService = authorizationService;
  }

  public void setMetrics(Metrics metrics) {
    this.metrics = metrics;
  }

  public void setHttpMetricsTagProvider(HttpMetricsTagProvider httpMetricsTagProvider) {
    this.httpMetricsTagProvider = httpMetricsTagProvider;
  }

  public void setDataStoreFactory(DataStoreFactory dataStoreFactory) {
    this.dataStoreFactory = dataStoreFactory;
  }

  public void start() throws Exception {
    RestApiServer server =
        new RestApiServer(
            this.authenticationService,
            this.authorizationService,
            this.metrics,
            this.httpMetricsTagProvider,
            dataStoreFactory);
    server.run("server", "restapi-config.yaml");
  }
}
