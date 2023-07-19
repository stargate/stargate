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
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.sgv2.api.common.properties.datastore;

/** Properties of the data store. */
public interface DataStoreProperties {

  /** @return If the secondary indexes are enabled. */
  boolean secondaryIndexesEnabled();

  /** @return If Storage attached indexes are enabled. */
  boolean saiEnabled();

  /** @return If vector search indexes are enabled. */
  boolean vectorSearchEnabled();

  /** @return If logged batches are enabled. */
  boolean loggedBatchesEnabled();

  /**
   * @return If boolean values should be treated as the numeric in the data store. By default <code>
   *     true</code> when {@link #saiEnabled()} is <code>true</code>.
   */
  default boolean treatBooleansAsNumeric() {
    return saiEnabled();
  }
}
