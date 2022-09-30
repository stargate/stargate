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
package io.stargate.bridge;

import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthorizationService;
import io.stargate.bridge.impl.BridgeImpl;
import io.stargate.core.activator.BaseActivator;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.db.DbActivator;
import io.stargate.db.Persistence;
import java.util.Arrays;
import java.util.List;
import org.jetbrains.annotations.Nullable;

public class BridgeActivator extends BaseActivator {
  private BridgeImpl bridge;
  private final ServicePointer<Metrics> metrics = ServicePointer.create(Metrics.class);
  private final ServicePointer<AuthenticationService> authentication =
      ServicePointer.create(
          AuthenticationService.class,
          "AuthIdentifier",
          System.getProperty("stargate.auth_id", "AuthTableBasedService"));
  private final ServicePointer<AuthorizationService> authorization =
      ServicePointer.create(AuthorizationService.class);
  private final ServicePointer<Persistence> persistence =
      ServicePointer.create(Persistence.class, "Identifier", DbActivator.PERSISTENCE_IDENTIFIER);

  public BridgeActivator() {
    super("bridge", true);
  }

  @Nullable
  @Override
  protected ServiceAndProperties createService() {
    if (bridge != null) { // Already started
      return null;
    }
    bridge =
        new BridgeImpl(persistence.get(), metrics.get(), authentication.get(), authorization.get());
    bridge.start();

    return null;
  }

  @Override
  protected void stopService() {
    if (bridge == null) { // Not started
      return;
    }
    bridge.stop();
    bridge = null;
  }

  @Override
  protected List<ServicePointer<?>> dependencies() {
    return Arrays.asList(metrics, persistence, authentication, authorization);
  }
}
