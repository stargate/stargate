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

package io.stargate.core.metrics.api;

import io.micrometer.core.instrument.Tags;
import java.util.List;
import java.util.Map;

/**
 * No-op implementation of the {@link HttpMetricsTagProvider}, returns empty tags for any request.
 */
public class NoopHttpMetricsTagProvider implements HttpMetricsTagProvider {

  /** {@inheritDoc} */
  @Override
  public Tags getRequestTags(Map<String, List<String>> headers) {
    return Tags.empty();
  }
}
