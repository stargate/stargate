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
import java.util.Collection;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.glassfish.jersey.server.ExtendedUriInfo;
import org.glassfish.jersey.server.monitoring.RequestEvent;

/**
 * A simple tag provider that overwrites the module tag to <code>other</code> if URI matches one of
 * the patterns.
 */
public class NonApiModuleTagsProvider implements JerseyTagsProvider {

  public static final String NON_API_MODULE_EXTENSION = "other";

  private final Tags tags;

  private final Collection<Pattern> uriPatterns;

  public NonApiModuleTagsProvider(Metrics metrics, String module, Collection<String> uriRegexes) {
    this.tags = metrics.tagsForModule(module + "-" + NON_API_MODULE_EXTENSION);
    this.uriPatterns = uriRegexes.stream().map(Pattern::compile).collect(Collectors.toList());
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
    if (isNonApiRequest(event.getUriInfo())) {
      return tags;
    }
    return Tags.empty();
  }

  // we are on non-api if any pattern matches
  private boolean isNonApiRequest(ExtendedUriInfo uriInfo) {
    if (uriPatterns.isEmpty()) {
      return false;
    }

    String path = uriInfo.getAbsolutePath().getPath();
    return uriPatterns.stream().anyMatch(p -> p.matcher(path).matches());
  }
}
