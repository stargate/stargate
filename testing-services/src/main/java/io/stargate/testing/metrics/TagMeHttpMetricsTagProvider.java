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

package io.stargate.testing.metrics;

import io.micrometer.core.instrument.Tags;
import io.stargate.core.metrics.api.HttpMetricsTagProvider;
import java.util.List;
import java.util.Map;

/**
 * Simple {@link HttpMetricsTagProvider} for testing that extracts {@value TAG_ME_HEADER} header and
 * converts to tag.
 */
public class TagMeHttpMetricsTagProvider implements HttpMetricsTagProvider {

  public static final String TAG_ME_HEADER = "x-tag-me";
  public static final String TAG_ME_KEY = "tagme";

  @Override
  public Tags getRequestTags(Map<String, List<String>> headers) {
    List<String> values = headers.get(TAG_ME_HEADER);
    if (null != values && !values.isEmpty()) {
      return Tags.of(TAG_ME_KEY, values.iterator().next());
    } else {
      return Tags.of(TAG_ME_KEY, "UNKNOWN");
    }
  }
}
