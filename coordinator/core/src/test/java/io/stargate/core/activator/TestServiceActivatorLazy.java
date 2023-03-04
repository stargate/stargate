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

import java.util.Collections;
import java.util.Hashtable;
import java.util.List;

public class TestServiceActivatorLazy extends BaseActivator {

  public boolean stopCalled;

  ServicePointer<DependentService1> service1 = ServicePointer.create(DependentService1.class);
  LazyServicePointer<DependentService2> service2 =
      LazyServicePointer.create(DependentService2.class);

  public TestServiceActivatorLazy() {
    super("Lazy Test Activator");
  }

  @Override
  protected List<ServicePointer<?>> dependencies() {
    return Collections.singletonList(service1);
  }

  @Override
  protected List<LazyServicePointer<?>> lazyDependencies() {
    return Collections.singletonList(service2);
  }

  @Override
  protected ServiceAndProperties createService() {
    @SuppressWarnings("JdkObsolete")
    Hashtable<String, String> props = new Hashtable<>();
    props.put("Identifier", "id_1");

    return new ServiceAndProperties(
        new TestServiceLazy(service1.get(), service2.get()), TestServiceLazy.class, props);
  }

  @Override
  protected void stopService() {
    stopCalled = true;
  }
}
