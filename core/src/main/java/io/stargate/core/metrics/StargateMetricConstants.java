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

package io.stargate.core.metrics;

import io.micrometer.core.instrument.Tag;

/** Contains all constant related to the micrometer metrics and tags. */
public class StargateMetricConstants {

  // metric names
  public static final String METRIC_HTTP_SERVER_REQUESTS = "http.server.requests";

  // tag keys
  public static final String MODULE_KEY = "module";

  // unknown value for any tag
  public static final String UNKNOWN = "unknown";

  // unknown tag instances
  public static final Tag TAG_MODULE_UNKNOWN = Tag.of(MODULE_KEY, UNKNOWN);
}
