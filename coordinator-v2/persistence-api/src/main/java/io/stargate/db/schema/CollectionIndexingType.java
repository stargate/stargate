/*
 * Copyright DataStax, Inc. and/or The Stargate Authors
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
package io.stargate.db.schema;

import java.io.Serializable;
import org.immutables.value.Value;

@Value.Immutable(prehash = true)
public abstract class CollectionIndexingType implements Serializable {
  @Value.Default
  public boolean indexKeys() {
    return false;
  }

  @Value.Default
  public boolean indexValues() {
    return false;
  }

  @Value.Default
  public boolean indexEntries() {
    return false;
  }

  @Value.Default
  public boolean indexFull() {
    return false;
  }
}
