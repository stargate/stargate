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
import io.stargate.auth.api.impl.WebImpl;
import io.stargate.core.activator.BaseActivator;
import io.stargate.core.metrics.api.Metrics;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthApiActivator extends BaseActivator {
  private static final Logger log = LoggerFactory.getLogger(AuthApiActivator.class);

  private final WebImpl web = new WebImpl();
  private final ServicePointer<Metrics> metric = ServicePointer.create(Metrics.class);
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
    web.setAuthenticationService(authenticationService.get());
    web.setMetrics(metric.get());
    try {
      this.web.start();
    } catch (Exception e) {
      log.error("Failed", e);
    }

    return null;
  }

  @Override
  protected void stopService() {
    // no-op
  }

  @Override
  protected List<ServicePointer<?>> dependencies() {
    return Arrays.asList(metric, authenticationService);
  }
}
