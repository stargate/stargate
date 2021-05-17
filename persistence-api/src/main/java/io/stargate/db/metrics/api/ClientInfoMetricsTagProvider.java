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

package io.stargate.db.metrics.api;

import io.micrometer.core.instrument.Tags;
import io.stargate.db.ClientInfo;

/**
 * Provides extra micrometer {@link Tags} based on the {@link ClientInfo}.
 *
 * <p>Note that each method implementation must always return constant amount of tags regardless of
 * the input.
 */
public interface ClientInfoMetricsTagProvider {

  /** Returns default interface implementation, which returns empty tags for all methods. */
  ClientInfoMetricsTagProvider DEFAULT = new ClientInfoMetricsTagProvider() {};

  /**
   * Returns tags for a {@link ClientInfo}.
   *
   * <p>Note that the implementation must return constant amount of tags for any input.
   *
   * @param clientInfo {@link ClientInfo}
   * @return Tags
   */
  default Tags getClientInfoTags(ClientInfo clientInfo) {
    return Tags.empty();
  }
}
