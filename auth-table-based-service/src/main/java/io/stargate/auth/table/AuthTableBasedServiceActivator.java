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
package io.stargate.auth.table;

import io.stargate.auth.AuthenticationService;
import io.stargate.core.activator.BaseActivator;
import io.stargate.db.Persistence;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;

public class AuthTableBasedServiceActivator extends BaseActivator {
  private final AuthTableBasedService authTableBasedService = new AuthTableBasedService();
  static Hashtable<String, String> props = new Hashtable<>();
  private final ServicePointer<Persistence> persistence =
      ServicePointer.create(Persistence.class, "stargate.persistence_id", "CassandraPersistence");

  static {
    props.put("AuthIdentifier", "AuthTableBasedService");
  }

  public AuthTableBasedServiceActivator() {
    super("authTableBasedService", AuthenticationService.class);
  }

  @Override
  protected ServiceAndProperties createService() {
    authTableBasedService.setPersistence(persistence.get());
    return new ServiceAndProperties(authTableBasedService, props);
  }

  @Override
  protected void stopService() {
    // no-op
  }

  @Override
  protected List<ServicePointer<?>> dependencies() {
    return Collections.singletonList(persistence);
  }
}
