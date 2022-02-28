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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.stargate.core.metrics.api.Metrics;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import org.apache.commons.lang3.RandomStringUtils;
import org.glassfish.jersey.server.ExtendedUriInfo;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class NonApiModuleTagsProviderTest {

  NonApiModuleTagsProvider provider;

  @Mock Metrics metrics;

  @Mock RequestEvent requestEvent;

  @Mock ExtendedUriInfo extendedUriInfo;

  @BeforeEach
  public void mockEvent() {
    lenient().when(requestEvent.getUriInfo()).thenReturn(extendedUriInfo);
  }

  @Nested
  class HttpRequestTags {

    @Test
    public void happyPath() throws Exception {
      Tags tags = Tags.of("module", RandomStringUtils.randomAlphanumeric(16));
      when(extendedUriInfo.getAbsolutePath()).thenReturn(new URI("/playground"));
      when(metrics.tagsForModule("testing-other")).thenReturn(tags);

      provider =
          new NonApiModuleTagsProvider(
              metrics, "testing", Collections.singleton("^/(playground|swagger)$"));
      Iterable<Tag> result = provider.httpRequestTags(requestEvent);

      assertThat(result).containsAll(tags);
      verify(metrics).tagsForModule("testing-other");
      verifyNoMoreInteractions(metrics);
    }

    @Test
    public void notMatched() throws Exception {
      when(extendedUriInfo.getAbsolutePath()).thenReturn(new URI("/v1/keyspaces/my-keyspace"));

      provider =
          new NonApiModuleTagsProvider(
              metrics, "testing", Arrays.asList("^/$", "^/playground$", "^/swagger.*"));
      Iterable<Tag> result = provider.httpRequestTags(requestEvent);

      assertThat(result).isEmpty();
      verify(metrics).tagsForModule("testing-other");
      verifyNoMoreInteractions(metrics);
    }

    @Test
    public void emptyRegexes() {
      provider = new NonApiModuleTagsProvider(metrics, "testing", Collections.emptyList());
      Iterable<Tag> result = provider.httpRequestTags(requestEvent);

      assertThat(result).isEmpty();
      verify(metrics).tagsForModule("testing-other");
      verifyNoMoreInteractions(metrics, extendedUriInfo);
    }
  }

  @Nested
  class HttpLongRequestTags {

    @Test
    public void happyPath() throws Exception {
      Tags tags = Tags.of("module", RandomStringUtils.randomAlphanumeric(16));
      when(extendedUriInfo.getAbsolutePath()).thenReturn(new URI("/playground"));
      when(metrics.tagsForModule("testing-other")).thenReturn(tags);

      provider =
          new NonApiModuleTagsProvider(metrics, "testing", Collections.singleton("^/playground$"));
      Iterable<Tag> result = provider.httpLongRequestTags(requestEvent);

      assertThat(result).containsAll(tags);
      verify(metrics).tagsForModule("testing-other");
      verifyNoMoreInteractions(metrics);
    }

    @Test
    public void notMatched() throws Exception {
      when(extendedUriInfo.getAbsolutePath()).thenReturn(new URI("/v1/keyspaces/my-keyspace"));

      provider =
          new NonApiModuleTagsProvider(
              metrics, "testing", Arrays.asList("^/$", "^/playground$", "^/swagger.*"));
      Iterable<Tag> result = provider.httpLongRequestTags(requestEvent);

      assertThat(result).isEmpty();
      verify(metrics).tagsForModule("testing-other");
      verifyNoMoreInteractions(metrics);
    }

    @Test
    public void emptyRegexes() {
      provider = new NonApiModuleTagsProvider(metrics, "testing", Collections.emptyList());
      Iterable<Tag> result = provider.httpLongRequestTags(requestEvent);

      assertThat(result).isEmpty();
      verify(metrics).tagsForModule("testing-other");
      verifyNoMoreInteractions(metrics, extendedUriInfo);
    }
  }
}
