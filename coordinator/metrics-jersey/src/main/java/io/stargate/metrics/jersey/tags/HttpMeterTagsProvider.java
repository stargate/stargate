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
import io.micrometer.jersey2.server.JerseyTags;
import io.micrometer.jersey2.server.JerseyTagsProvider;
import io.stargate.core.metrics.api.HttpMetricsTagProvider;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.ContainerResponse;
import org.glassfish.jersey.server.monitoring.RequestEvent;

/**
 * The tag provider for the Jersey HTTP request when metering requests. Can contain an optional
 * {@link HttpMetricsTagProvider}.
 *
 * @see RequestEvent
 * @see io.micrometer.jersey2.server.DefaultJerseyTagsProvider
 */
public class HttpMeterTagsProvider implements JerseyTagsProvider {

  private final HttpMetricsTagProvider httpMetricsTagProvider;

  public HttpMeterTagsProvider() {
    this(null);
  }

  public HttpMeterTagsProvider(HttpMetricsTagProvider httpMetricsTagProvider) {
    this.httpMetricsTagProvider = httpMetricsTagProvider;
  }

  /** {@inheritDoc} */
  @Override
  public Iterable<Tag> httpRequestTags(RequestEvent event) {
    // adds method, uri, status and request tags
    ContainerResponse response = event.getContainerResponse();
    ContainerRequest request = event.getContainerRequest();

    Tags result =
        Tags.of(JerseyTags.method(request), JerseyTags.uri(event), JerseyTags.status(response));

    if (null != httpMetricsTagProvider) {
      return result.and(
          httpMetricsTagProvider.getRequestTags(event.getContainerRequest().getHeaders()));
    } else {
      return result;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Iterable<Tag> httpLongRequestTags(RequestEvent event) {
    // adds method, uri and request tags
    ContainerRequest request = event.getContainerRequest();

    Tags result = Tags.of(JerseyTags.method(request), JerseyTags.uri(event));

    if (null != httpMetricsTagProvider) {
      return result.and(
          httpMetricsTagProvider.getRequestTags(event.getContainerRequest().getHeaders()));
    } else {
      return result;
    }
  }
}
