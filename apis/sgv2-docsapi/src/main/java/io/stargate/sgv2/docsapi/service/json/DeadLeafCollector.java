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
package io.stargate.sgv2.docsapi.service.json;

/**
 * Interface that collects paths in a document that have outdated write-times, and are therefore
 * unused. Such collections can then be used to prune the data that is no longer reachable via a
 * delete on the underlying datastore.
 */
public interface DeadLeafCollector {

  /**
   * Adds a dead leaf to the collector.
   *
   * @param path the path that the DeadLeaf lives on, e.g. "a.b.c"
   * @param leaf the DeadLeaf.
   */
  default void addLeaf(String path, DeadLeaf leaf) {}

  /**
   * Add to the collector a representation that an entire array at a path is no longer reachable and
   * should be deleted.
   *
   * @param path the path of the array, e.g. "a.b.c"
   */
  default void addArray(String path) {}

  /**
   * Add to the collector a representation that all the data at a path is no longer reachable and
   * should be deleted.
   *
   * @param path the path of the data, e.g. "a.b.c"
   */
  default void addAll(String path) {}

  /** Returns true if the collector is empty. */
  default boolean isEmpty() {
    return true;
  }
}
