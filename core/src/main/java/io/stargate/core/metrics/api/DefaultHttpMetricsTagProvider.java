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

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Default {@link HttpMetricsTagProvider} that adds headers as tags based on the whitelisted header
 * names from the system prop.
 */
public class DefaultHttpMetricsTagProvider implements HttpMetricsTagProvider {

  private final Config config;

  public DefaultHttpMetricsTagProvider() {
    this(Config.fromSystemProps());
  }

  public DefaultHttpMetricsTagProvider(Config config) {
    this.config = config;
  }

  /** {@inheritDoc} */
  @Override
  public Tags getRequestTags(Map<String, List<String>> headers) {
    Collection<String> whitelist = config.whitelistedHeaderNames;
    if (null == whitelist || whitelist.isEmpty()) {
      return Tags.empty();
    } else {
      Tag[] collect =
          headers.entrySet().stream()
              .filter(e -> whitelist.contains(e.getKey().toLowerCase()))
              .map(e -> Tag.of(e.getKey(), String.join(",", e.getValue())))
              .toArray(Tag[]::new);
      return Tags.of(collect);
    }
  }

  public static class Config {

    private final Collection<String> whitelistedHeaderNames;

    public Config(Collection<String> whitelistedHeaderNames) {
      this.whitelistedHeaderNames = whitelistedHeaderNames;
    }

    public static Config fromSystemProps() {
      try {
        String property = System.getProperty("stargate.metrics.http_server_requests_header_tags");
        if (null != property) {
          String[] headers = property.split(",");
          List<String> lowercaseHeaders =
              Arrays.stream(headers).map(String::toLowerCase).collect(Collectors.toList());
          return new Config(lowercaseHeaders);
        } else {
          return new Config(Collections.emptyList());
        }
      } catch (Exception e) {
        return new Config(Collections.emptyList());
      }
    }
  }
}
