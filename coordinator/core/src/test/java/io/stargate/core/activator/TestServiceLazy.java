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

import java.util.concurrent.atomic.AtomicReference;

public class TestServiceLazy {
  private final DependentService1 dependentService1;
  private final AtomicReference<DependentService2> dependentService2;

  public TestServiceLazy(
      DependentService1 dependentService1, AtomicReference<DependentService2> dependentService2) {

    this.dependentService1 = dependentService1;
    this.dependentService2 = dependentService2;
  }

  public DependentService1 getDependentService1() {
    return dependentService1;
  }

  public AtomicReference<DependentService2> getDependentService2() {
    return dependentService2;
  }
}
