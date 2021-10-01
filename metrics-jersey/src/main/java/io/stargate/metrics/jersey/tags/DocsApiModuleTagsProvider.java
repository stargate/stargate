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
import io.stargate.core.metrics.api.Metrics;
import org.glassfish.jersey.server.ExtendedUriInfo;
import org.glassfish.jersey.server.monitoring.RequestEvent;

/**
 * A simple tag provider that overwrites the module tags if the HTTP request is targeting our Docs
 * API.
 */
public class DocsApiModuleTagsProvider implements JerseyTagsProvider {

  public static final String DOCS_API_MODULE_NAME = "docsapi";

  private final Metrics metrics;

  public DocsApiModuleTagsProvider(Metrics metrics) {
    this.metrics = metrics;
  }

  /** {@inheritDoc} */
  @Override
  public Iterable<Tag> httpRequestTags(RequestEvent event) {
    return tagsInternal(event);
  }

  /** {@inheritDoc} */
  @Override
  public Iterable<Tag> httpLongRequestTags(RequestEvent event) {
    return tagsInternal(event);
  }

  private Iterable<Tag> tagsInternal(RequestEvent event) {
    if (isDocsApiRequest(event.getUriInfo())) {
      return metrics.tagsForModule(DOCS_API_MODULE_NAME);
    }
    return Tags.empty();
  }

  // we are on docs api if there is /namespaces in the URL path
  private boolean isDocsApiRequest(ExtendedUriInfo uriInfo) {
    return uriInfo.getPath(true).contains("/namespaces");
  }
}
