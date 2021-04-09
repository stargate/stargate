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

package io.stargate.health.metrics;

import io.micrometer.core.instrument.Tag;
import io.micrometer.jersey2.server.JerseyTags;
import io.micrometer.jersey2.server.JerseyTagsProvider;
import io.stargate.core.metrics.StargateTenantExtractor;
import io.stargate.core.metrics.api.Metrics;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.ContainerResponse;
import org.glassfish.jersey.server.monitoring.RequestEvent;

/**
 * Tag provider for the Jersey HTTP request.
 *
 * @see RequestEvent
 * @see io.micrometer.jersey2.server.DefaultJerseyTagsProvider
 */
public class HealthCheckerTagsProvider implements JerseyTagsProvider {

  private final Metrics metrics;

  private final String module;

  public HealthCheckerTagsProvider(Metrics metrics, String module) {
    this.metrics = metrics;
    this.module = module;
  }

  /** {@inheritDoc} */
  @Override
  public Iterable<Tag> httpRequestTags(RequestEvent event) {
    // adds method, uri, status and tenant & module
    ContainerResponse response = event.getContainerResponse();
    ContainerRequest containerRequest = event.getContainerRequest();
    String tenant =
        StargateTenantExtractor.tenantFromHeaders(event.getContainerRequest().getHeaders());

    return metrics
        .tagsWithTenant(module, tenant)
        .and(
            JerseyTags.method(containerRequest),
            JerseyTags.uri(event),
            JerseyTags.status(response));
  }

  /** {@inheritDoc} */
  @Override
  public Iterable<Tag> httpLongRequestTags(RequestEvent event) {
    // adds method, uri and tenant & module, omit status in order not to depend on the response
    ContainerRequest containerRequest = event.getContainerRequest();
    String tenant =
        StargateTenantExtractor.tenantFromHeaders(event.getContainerRequest().getHeaders());

    return metrics
        .tagsWithTenant(module, tenant)
        .and(JerseyTags.method(containerRequest), JerseyTags.uri(event));
  }
}
