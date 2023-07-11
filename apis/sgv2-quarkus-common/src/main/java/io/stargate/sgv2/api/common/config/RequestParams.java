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
package io.stargate.sgv2.api.common.config;

import org.immutables.value.Value.Immutable;

/** This holds the request parameters that are common to the API requests. */
@Immutable
public interface RequestParams {
  /**
   * This is an option set by the client in an API call to get the map data during a read operation
   * or to specify the format of the map data during a write operation. if true
   *
   * <pre>
   *     "map": {
   *        "key1": "value1",
   *        "key2": "value2"
   *      }
   * </pre>
   *
   * else
   *
   * <pre>
   *      "map": [
   *      {
   *        "key": "key1",
   *        "value": "value1"
   *      },
   *      {
   *        "key": "key2",
   *        "value": "value2"
   *      }
   *      ]
   * </pre>
   *
   * @return boolean
   */
  boolean compactMapData();
}
