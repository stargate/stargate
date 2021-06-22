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
import java.util.Collection;
import java.util.Collections;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.ContainerResponse;
import org.glassfish.jersey.server.monitoring.RequestEvent;

/**
 * The main tag provider for the Jersey HTTP request. Can contain other delegating providers as
 * well.
 *
 * @see RequestEvent
 * @see io.micrometer.jersey2.server.DefaultJerseyTagsProvider
 */
public class ResourceTagsProvider implements JerseyTagsProvider {

  private final HttpMetricsTagProvider httpMetricsTagProvider;

  private final Tags defaultTags;

  private final Collection<JerseyTagsProvider> delegateProviders;

  public ResourceTagsProvider(HttpMetricsTagProvider httpMetricsTagProvider, Tags defaultTags) {
    this(httpMetricsTagProvider, defaultTags, Collections.emptyList());
  }

  public ResourceTagsProvider(
      HttpMetricsTagProvider httpMetricsTagProvider,
      Tags defaultTags,
      Collection<JerseyTagsProvider> delegateProviders) {
    this.httpMetricsTagProvider = httpMetricsTagProvider;
    this.defaultTags = defaultTags;
    this.delegateProviders = delegateProviders;
  }

  /** {@inheritDoc} */
  @Override
  public Iterable<Tag> httpRequestTags(RequestEvent event) {
    // adds method, uri, status, default and request tags
    ContainerResponse response = event.getContainerResponse();
    ContainerRequest request = event.getContainerRequest();
    Tags result = defaultTags;

    // resolve delegates
    for (JerseyTagsProvider delegate : delegateProviders) {
      result = result.and(delegate.httpRequestTags(event));
    }

    // then from the HttpMetricsTagProvider
    Tags requestTags =
        httpMetricsTagProvider.getRequestTags(event.getContainerRequest().getHeaders());

    return result
        .and(requestTags)
        .and(JerseyTags.method(request), JerseyTags.uri(event), JerseyTags.status(response));
  }

  /** {@inheritDoc} */
  @Override
  public Iterable<Tag> httpLongRequestTags(RequestEvent event) {
    // adds method, uri, default and request tags
    ContainerRequest containerRequest = event.getContainerRequest();
    Tags result = defaultTags;

    // resolve delegates
    for (JerseyTagsProvider delegate : delegateProviders) {
      result = result.and(delegate.httpLongRequestTags(event));
    }

    // then from the HttpMetricsTagProvider
    Tags requestTags =
        httpMetricsTagProvider.getRequestTags(event.getContainerRequest().getHeaders());

    return result.and(requestTags).and(JerseyTags.method(containerRequest), JerseyTags.uri(event));
  }
}
