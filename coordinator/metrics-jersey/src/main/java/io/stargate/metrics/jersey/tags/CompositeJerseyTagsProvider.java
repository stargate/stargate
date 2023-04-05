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
import java.util.List;
import org.glassfish.jersey.server.monitoring.RequestEvent;

/** The composite {@link JerseyTagsProvider} that collects tags from multiple providers. */
public class CompositeJerseyTagsProvider implements JerseyTagsProvider {

  private final List<JerseyTagsProvider> delegateProviders;

  /**
   * Default constructor.
   *
   * @param delegateProviders Delegate providers. Note that the order specified does matter, as a
   *     higher index provider will overwrite any tags with same keys provided by a provider with
   *     lower index.
   */
  public CompositeJerseyTagsProvider(List<JerseyTagsProvider> delegateProviders) {
    this.delegateProviders = delegateProviders;
  }

  /** {@inheritDoc} */
  @Override
  public Iterable<Tag> httpRequestTags(RequestEvent event) {
    Tags tags = Tags.empty();

    for (JerseyTagsProvider provider : delegateProviders) {
      tags = tags.and(provider.httpRequestTags(event));
    }

    return tags;
  }

  /** {@inheritDoc} */
  @Override
  public Iterable<Tag> httpLongRequestTags(RequestEvent event) {
    Tags tags = Tags.empty();

    for (JerseyTagsProvider provider : delegateProviders) {
      tags = tags.and(provider.httpLongRequestTags(event));
    }

    return tags;
  }
}
