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

package io.stargate.metrics.jersey.tags;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.jersey2.server.JerseyTagsProvider;
import io.stargate.core.metrics.StargateMetricConstants;
import io.stargate.core.metrics.api.HttpMetricsTagProvider;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.glassfish.jersey.server.monitoring.RequestEvent;

/**
 * Default {@link HttpMetricsTagProvider} that adds headers as tags based on the whitelisted header
 * names from the system prop.
 */
public class HeadersTagProvider implements JerseyTagsProvider {

  private final Config config;

  public HeadersTagProvider() {
    this(Config.fromSystemProps());
  }

  public HeadersTagProvider(Config config) {
    this.config = config;
  }

  /** {@inheritDoc} */
  @Override
  public Iterable<Tag> httpRequestTags(RequestEvent event) {
    return getRequestTags(event.getContainerRequest().getHeaders());
  }

  /** {@inheritDoc} */
  @Override
  public Iterable<Tag> httpLongRequestTags(RequestEvent event) {
    return getRequestTags(event.getContainerRequest().getHeaders());
  }

  private Tags getRequestTags(Map<String, List<String>> headers) {
    Collection<String> whitelist = config.whitelistedHeaderNames;
    if (null == whitelist || whitelist.isEmpty()) {
      return Tags.empty();
    } else {
      // note that we need to return same set of tags
      // this is why we iterate white list and try to find header value
      Tag[] collect =
          whitelist.stream()
              .map(
                  header ->
                      headers.entrySet().stream()
                          .filter(e -> Objects.equals(header, e.getKey().toLowerCase()))
                          .findAny()
                          .map(e -> Tag.of(header, String.join(",", e.getValue())))
                          .orElseGet(() -> Tag.of(header, StargateMetricConstants.UNKNOWN)))
              .toArray(Tag[]::new);

      return Tags.of(collect);
    }
  }

  public static class Config {

    private final Collection<String> whitelistedHeaderNames;

    public Config(Collection<String> whitelistedHeaderNames) {
      if (null == whitelistedHeaderNames) {
        this.whitelistedHeaderNames = Collections.emptyList();
      } else {
        this.whitelistedHeaderNames = whitelistedHeaderNames;
      }
    }

    public static Config fromSystemProps() {
      String property = System.getProperty("stargate.metrics.http_server_requests_header_tags");
      return fromPropertyString(property);
    }

    public static Config fromPropertyString(String value) {
      try {
        if (null != value && value.length() > 0) {
          String[] headers = value.split(",");
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
