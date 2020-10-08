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
package io.stargate.it.storage;

/** Defines lifecycle scopes for backend clusters used by tests. */
public enum ClusterScope {
  /**
   * The cluster will be shared across tests using this scope. If a test requires a cluster with
   * different properties, the shared instance will be shut down. So, to minimize restarts tests
   * using the shared cluster should be grouped together during execution.
   *
   * <p>Note: all cluster parameters should have default values for the shared cluster.
   */
  SHARED,

  /**
   * A cluster instance per test class. Corresponds to {@link org.junit.jupiter.api.BeforeAll} and
   * {@link org.junit.jupiter.api.AfterAll} callbacks.
   */
  CLASS,
}
