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

  public TestServiceActivator() {
    super(
        "Config Store Test Activator",
        Arrays.asList(
            DependentService.constructDependentService(DependentService1.class),
            DependentService.constructDependentService(DependentService2.class)),
        TestService.class);
  }

  @Override
  protected ServiceAndProperties createService(List<Object> dependentServices) {
    DependentService1 dependentService1 = (DependentService1) dependentServices.get(0);
    DependentService2 dependentService2 = (DependentService2) dependentServices.get(1);

    Hashtable<String, String> props = new Hashtable<>();
    props.put("Identifier", "id_1");

    return new ServiceAndProperties(new TestService(dependentService1, dependentService2), props);
  }

  @Override
  protected void stopService() {
    stopCalled = true;
  }
}
