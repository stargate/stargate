/*
 * Copyright The Stargate Authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.stargate.core.metrics.api;

import io.micrometer.core.instrument.Tags;
import java.util.List;
import java.util.Map;

/**
 * Provides extra micrometer {@link Tags} for user requests coming from the HTTP.
 *
 * <p>Note that each method implementation must always return constant amount of tags regardless of
 * the input.
 */
public interface HttpMetricsTagProvider {

  /** Returns default interface implementation, which returns empty tags for all methods. */
  HttpMetricsTagProvider DEFAULT = new HttpMetricsTagProvider() {};

  /**
   * Returns tags for a HTTP request, usually extracted from the given headers.
   *
   * <p>Nota that the implementation must return constant amount of tags for any input.
   *
   * @param headers HTTP Headers
   * @return Tags
   */
  default Tags getRequestTags(Map<String, List<String>> headers) {
    return Tags.empty();
  }
}
