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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.jersey2.server.JerseyTagsProvider;
import java.util.Arrays;
import java.util.Collections;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CompositeJerseyTagsProviderTest {

  @Mock RequestEvent requestEvent;

  @Mock JerseyTagsProvider delegate1;

  @Mock JerseyTagsProvider delegate2;

  @Nested
  class HttpRequestTags {

    @Test
    public void happyPath() {
      Tags tags1 = Tags.of("one", "two");
      Tags tags2 = Tags.of("three", "four");
      when(delegate1.httpRequestTags(requestEvent)).thenReturn(tags1);
      when(delegate2.httpRequestTags(requestEvent)).thenReturn(tags2);

      CompositeJerseyTagsProvider provider =
          new CompositeJerseyTagsProvider(Arrays.asList(delegate1, delegate2));
      Iterable<Tag> result = provider.httpRequestTags(requestEvent);

      assertThat(result).hasSize(2).containsAll(tags1).containsAll(tags2);
      verify(delegate1).httpRequestTags(requestEvent);
      verify(delegate2).httpRequestTags(requestEvent);
      verifyNoMoreInteractions(delegate1, delegate2);
    }

    @Test
    public void emptyList() {
      CompositeJerseyTagsProvider provider =
          new CompositeJerseyTagsProvider(Collections.emptyList());
      Iterable<Tag> result = provider.httpRequestTags(requestEvent);

      assertThat(result).hasSize(0);
      verifyNoMoreInteractions(delegate1, delegate2);
    }
  }

  @Nested
  class HttpLongRequestTags {

    @Test
    public void happyPath() {
      Tags tags1 = Tags.empty();
      Tags tags2 = Tags.of("three", "four");
      when(delegate1.httpLongRequestTags(requestEvent)).thenReturn(tags1);
      when(delegate2.httpLongRequestTags(requestEvent)).thenReturn(tags2);

      CompositeJerseyTagsProvider provider =
          new CompositeJerseyTagsProvider(Arrays.asList(delegate1, delegate2));
      Iterable<Tag> result = provider.httpLongRequestTags(requestEvent);

      assertThat(result).hasSize(1).containsAll(tags1).containsAll(tags2);
      verify(delegate1).httpLongRequestTags(requestEvent);
      verify(delegate2).httpLongRequestTags(requestEvent);
      verifyNoMoreInteractions(delegate1, delegate2);
    }

    @Test
    public void emptyList() {
      CompositeJerseyTagsProvider provider =
          new CompositeJerseyTagsProvider(Collections.emptyList());
      Iterable<Tag> result = provider.httpLongRequestTags(requestEvent);

      assertThat(result).hasSize(0);
      verifyNoMoreInteractions(delegate1, delegate2);
    }
  }
}
