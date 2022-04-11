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
package io.stargate.sgv2.common.grpc;

import java.util.function.Supplier;

/**
 * A references that memoizes a value, but the computation function is provided by callers of {@link
 * #get(Supplier)}.
 */
class LazyReference<T> {

  private volatile T value;

  T get(Supplier<T> initializer) {
    T result = value;
    if (result != null) {
      return result;
    }
    synchronized (this) {
      if (value == null) {
        value = initializer.get();
      }
      return value;
    }
  }
}
