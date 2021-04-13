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

package io.stargate.metrics.jersey;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.jersey2.server.JerseyTags;
import io.micrometer.jersey2.server.JerseyTagsProvider;
import io.stargate.core.metrics.api.HttpMetricsTagProvider;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.ContainerResponse;
import org.glassfish.jersey.server.monitoring.RequestEvent;

/**
 * Tag provider for the Jersey HTTP request.
 *
 * @see RequestEvent
 * @see io.micrometer.jersey2.server.DefaultJerseyTagsProvider
 */
public class ResourceTagsProvider implements JerseyTagsProvider {

  private final HttpMetricsTagProvider httpMetricsTagProvider;

  private final Tags defaultTags;

  public ResourceTagsProvider(HttpMetricsTagProvider httpMetricsTagProvider, Tags defaultTags) {
    this.httpMetricsTagProvider = httpMetricsTagProvider;
    this.defaultTags = defaultTags;
  }

  /** {@inheritDoc} */
  @Override
  public Iterable<Tag> httpRequestTags(RequestEvent event) {
    // adds method, uri, status and tenant & module
    ContainerResponse response = event.getContainerResponse();
    ContainerRequest request = event.getContainerRequest();

    Tags requestTags =
        httpMetricsTagProvider.getRequestTags(event.getContainerRequest().getHeaders());

    return defaultTags
        .and(requestTags)
        .and(
            JerseyTags.method(request),
            JerseyTags.uri(event),
            JerseyTags.status(response));
  }

  /** {@inheritDoc} */
  @Override
  public Iterable<Tag> httpLongRequestTags(RequestEvent event) {
    // adds method, uri and tenant & module, omit status in order not to depend on the response
    ContainerRequest containerRequest = event.getContainerRequest();

    Tags requestTags =
        httpMetricsTagProvider.getRequestTags(event.getContainerRequest().getHeaders());

    return defaultTags
        .and(requestTags)
        .and(JerseyTags.method(containerRequest), JerseyTags.uri(event));
  }
}
