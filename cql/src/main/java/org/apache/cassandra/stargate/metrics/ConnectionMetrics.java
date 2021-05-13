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

package org.apache.cassandra.stargate.metrics;

import io.micrometer.core.instrument.Tags;

/** Interface that each connection can use to report metric or determine it Tags. */
public interface ConnectionMetrics {

  /** @return Returns micrometer tags associated with the connection. */
  Tags getTags();

  /** Marks request processed (increases the count). */
  void markRequestProcessed();

  /** Marks request discarded (increases the count). */
  void markRequestDiscarded();

  /** Marks auth success (increases the count). */
  void markAuthSuccess();

  /** Marks auth failure (increases the count). */
  void markAuthFailure();

  /** Marks auth error (increases the count). */
  void markAuthError();
}
