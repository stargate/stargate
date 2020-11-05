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
package io.stargate.core.activator;

import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;

public class TestServiceActivator extends BaseActivator {

  public boolean stopCalled;

  ServiceDependency<DependentService1> service1 = ServiceDependency.create(DependentService1.class);
  ServiceDependency<DependentService2> service2 = ServiceDependency.create(DependentService2.class);

  @Override
  protected List<ServiceDependency<?>> dependencies() {
    return Arrays.asList(service1, service2);
  }

  public TestServiceActivator() {
    this(TestService.class);
  }

  public TestServiceActivator(Class<?> targetService) {
    super("Config Store Test Activator", targetService);
  }

  @Override
  protected ServiceAndProperties createService() {
    Hashtable<String, String> props = new Hashtable<>();
    props.put("Identifier", "id_1");

    return new ServiceAndProperties(
        new TestService(service1.getService(), service2.getService()), props);
  }

  @Override
  protected void stopService() {
    stopCalled = true;
  }
}
