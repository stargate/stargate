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
import io.micrometer.jersey2.server.JerseyTagsProvider;
import io.stargate.core.metrics.StargateMetricConstants;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.ws.rs.core.MultivaluedMap;
import org.glassfish.jersey.server.ExtendedUriInfo;
import org.glassfish.jersey.server.monitoring.RequestEvent;

/** {@link JerseyTagsProvider} that can extract path params as tags. */
public class PathParametersTagsProvider implements JerseyTagsProvider {

  private final Config config;

  public PathParametersTagsProvider() {
    this(Config.fromSystemProps());
  }

  public PathParametersTagsProvider(Config config) {
    this.config = config;
  }

  /** {@inheritDoc} */
  @Override
  public Iterable<Tag> httpRequestTags(RequestEvent event) {
    if (config.whitelistedPathParams.isEmpty()) {
      return Collections.emptyList();
    }

    return pathParamTags(event);
  }

  /** {@inheritDoc} */
  @Override
  public Iterable<Tag> httpLongRequestTags(RequestEvent event) {
    if (config.whitelistedPathParams.isEmpty()) {
      return Collections.emptyList();
    }

    return pathParamTags(event);
  }

  private Iterable<Tag> pathParamTags(RequestEvent event) {
    ExtendedUriInfo uriInfo = event.getUriInfo();
    MultivaluedMap<String, String> pathParameters = uriInfo.getPathParameters(true);

    return config.whitelistedPathParams.stream()
        .map(
            param ->
                Optional.ofNullable(pathParameters.get(param))
                    .map(values -> Tag.of(param, String.join(",", values)))
                    .orElseGet(() -> Tag.of(param, StargateMetricConstants.UNKNOWN)))
        .collect(Collectors.toList());
  }

  // simple configuration, default based on the
  // stargate.metrics.http_server_requests_path_param_tags
  public static class Config {

    private final Collection<String> whitelistedPathParams;

    public Config(Collection<String> whitelistedPathParams) {
      if (null == whitelistedPathParams) {
        this.whitelistedPathParams = Collections.emptyList();
      } else {
        this.whitelistedPathParams = whitelistedPathParams;
      }
    }

    public static Config fromSystemProps() {
      String property = System.getProperty("stargate.metrics.http_server_requests_path_param_tags");
      return fromPropertyValue(property);
    }

    public static Config fromPropertyValue(String value) {
      if (null == value || value.length() == 0) {
        return new Config(Collections.emptyList());
      } else {
        return new Config(Arrays.asList(value.split(",")));
      }
    }
  }
}
