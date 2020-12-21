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
package io.stargate.testing;

import io.stargate.auth.AuthorizationProcessor;
import io.stargate.core.activator.BaseActivator;
import io.stargate.testing.auth.AuthorizationProcessorImpl;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;

public class TestingServicesActivator extends BaseActivator {

  private static final String TEST_AUTHZ_PROCESSOR_ID = "TestAuthzProcessor";

  public TestingServicesActivator() {
    super("testing-services");
  }

  @Override
  protected List<ServiceAndProperties> createServices() {
    if (TEST_AUTHZ_PROCESSOR_ID.equals(System.getProperty("stargate.authorization.processor.id"))) {
      AuthorizationProcessorImpl authzProcessor = new AuthorizationProcessorImpl();

      Hashtable<String, String> props = new Hashtable<>();
      props.put("AuthProcessorId", TEST_AUTHZ_PROCESSOR_ID);

      return Collections.singletonList(
          new ServiceAndProperties(authzProcessor, AuthorizationProcessor.class, props));
    }

    return Collections.emptyList();
  }

  @Override
  protected void stopService() {}

  @Override
  protected List<ServicePointer<?>> dependencies() {
    return Collections.emptyList();
  }
}
