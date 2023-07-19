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

package io.stargate.sgv2.restapi.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/** Configuration for the REST API. */
@ConfigMapping(prefix = "stargate.rest")
public interface RestApiConfig {

  /**
   * Flag to control map data optimization. When this is set to {@code true} the map data is
   * returned / accepted as a map of key-value pairs. For example:
   *
   * <pre>
   *     "map": {
   *        "key1": "value1",
   *        "key2": "value2"
   *      }
   *  </pre>
   *
   * When this ise to {@code false} the map data is returned / accepted as a list of map of pairs as
   * in the example below
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
   *  </pre>
   *
   * Note: this is a server side flag and the client can override this in different APIs using the
   * query param {@code RestOpenApiConstants.Parameters.COMPACT_MAP_DATA}
   */
  @WithDefault(RestApiConstants.COMPACT_MAP_DATA)
  boolean compactMapData();

  /** Flag to either enable or disable CQL over REST API. */
  @WithDefault(RestApiConstants.CQL_DISABLED)
  boolean cqlDisabled();
}
