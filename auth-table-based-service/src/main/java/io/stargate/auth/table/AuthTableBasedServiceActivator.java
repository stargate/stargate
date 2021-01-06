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
import io.stargate.auth.AuthorizationService;
import io.stargate.core.activator.BaseActivator;
import io.stargate.db.datastore.DataStoreFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import net.jcip.annotations.GuardedBy;

public class AuthTableBasedServiceActivator extends BaseActivator {
  @SuppressWarnings("JdkObsolete")
  static Hashtable<String, String> props = new Hashtable<>();

  private final ServicePointer<DataStoreFactory> dataStoreFactory =
      ServicePointer.create(DataStoreFactory.class);
  public static final String AUTH_TABLE_IDENTIFIER = "AuthTableBasedService";

  static {
    props.put("AuthIdentifier", AUTH_TABLE_IDENTIFIER);
  }

  public AuthTableBasedServiceActivator() {
    super("authnTableBasedService and authzTableBasedServie");
  }

  @GuardedBy("this")
  private final AuthnTableBasedService authnTableBasedService = new AuthnTableBasedService();

  @GuardedBy("this")
  private final AuthzTableBasedService authzTableBasedService = new AuthzTableBasedService();

  @Override
  // The parent class calls createServices() from a synchronized method
  @SuppressWarnings("GuardedBy")
  protected List<ServiceAndProperties> createServices() {
    if (AUTH_TABLE_IDENTIFIER.equals(
        System.getProperty("stargate.auth_id", AUTH_TABLE_IDENTIFIER))) {
      authnTableBasedService.setDataStoreFactory(dataStoreFactory.get());

      return Arrays.asList(
          new ServiceAndProperties(authnTableBasedService, AuthenticationService.class, props),
          new ServiceAndProperties(authzTableBasedService, AuthorizationService.class, props));
    }
    return Collections.emptyList();
  }

  @Override
  protected List<ServicePointer<?>> dependencies() {
    return Collections.singletonList(dataStoreFactory);
  }
}
