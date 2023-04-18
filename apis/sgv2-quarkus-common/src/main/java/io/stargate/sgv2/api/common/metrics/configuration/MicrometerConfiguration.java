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

package io.stargate.sgv2.api.common.metrics.configuration;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.config.MeterFilter;
import io.stargate.sgv2.api.common.config.MetricsConfig;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;

/** Configuration of all {@link MeterFilter}s used. */
public class MicrometerConfiguration {

  /** @return Produces meter filter that takes care of the global tags */
  @Produces
  @Singleton
  public MeterFilter globalTagsMeterFilter(MetricsConfig config) {
    Map<String, String> globalTags = config.globalTags();

    // if we have no global tags, use empty
    if (null == globalTags || globalTags.isEmpty()) {
      return new MeterFilter() {};
    }

    // transform to tags
    Collection<Tag> tags =
        globalTags.entrySet().stream()
            .map(e -> Tag.of(e.getKey(), e.getValue()))
            .collect(Collectors.toList());

    // return all
    return MeterFilter.commonTags(Tags.of(tags));
  }
}
