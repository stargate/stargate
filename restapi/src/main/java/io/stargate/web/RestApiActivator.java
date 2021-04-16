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
package io.stargate.web;

import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthorizationService;
import io.stargate.core.activator.BaseActivator;
import io.stargate.core.metrics.api.HttpMetricsTagProvider;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.web.impl.WebImpl;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Activator for the restapi bundle */
public class RestApiActivator extends BaseActivator {

  public static final String MODULE_NAME = "restapi";
  private static final Logger logger = LoggerFactory.getLogger(RestApiActivator.class);
  private final WebImpl web = new WebImpl();
  private final ServicePointer<AuthenticationService> authenticationService =
      ServicePointer.create(
          AuthenticationService.class,
          "AuthIdentifier",
          System.getProperty("stargate.auth_id", "AuthTableBasedService"));
  private final ServicePointer<Metrics> metrics = ServicePointer.create(Metrics.class);
  private final ServicePointer<HttpMetricsTagProvider> httpTagProvider =
      ServicePointer.create(HttpMetricsTagProvider.class);
  private final ServicePointer<AuthorizationService> authorizationService =
      ServicePointer.create(AuthorizationService.class);

  private final ServicePointer<DataStoreFactory> dataStoreFactory =
      ServicePointer.create(DataStoreFactory.class);

  public RestApiActivator() {
    super("restapi", true);
  }

  @Override
  protected ServiceAndProperties createService() {
    web.setAuthenticationService(authenticationService.get());
    web.setMetrics(metrics.get());
    web.setHttpMetricsTagProvider(httpTagProvider.get());
    web.setAuthorizationService(authorizationService.get());
    web.setDataStoreFactory(dataStoreFactory.get());
    try {
      this.web.start();
    } catch (Exception e) {
      logger.error("Failed", e);
    }
    return null;
  }

  @Override
  protected List<ServicePointer<?>> dependencies() {
    return Arrays.asList(
        authenticationService, metrics, httpTagProvider, authorizationService, dataStoreFactory);
  }
}
