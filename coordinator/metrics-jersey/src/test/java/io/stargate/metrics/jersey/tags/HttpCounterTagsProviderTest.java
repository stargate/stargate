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
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.stargate.core.metrics.api.HttpMetricsTagProvider;
import javax.ws.rs.core.MultivaluedMap;
import org.glassfish.jersey.internal.util.collection.MultivaluedStringMap;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.ContainerResponse;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class HttpCounterTagsProviderTest {

  @Mock HttpMetricsTagProvider httpMetricsTagProvider;

  @Mock RequestEvent requestEvent;

  @Mock ContainerRequest containerRequest;

  @Mock ContainerResponse containerResponse;

  @BeforeEach
  public void mockEvent() {
    lenient().when(requestEvent.getContainerRequest()).thenReturn(containerRequest);
    lenient().when(requestEvent.getContainerResponse()).thenReturn(containerResponse);
  }

  @Nested
  class HttpRequestTags {

    @Test
    public void happyPath() {
      MultivaluedMap<String, String> headers = new MultivaluedStringMap();
      headers.putSingle("some", "header");

      when(containerResponse.getStatus()).thenReturn(500);
      when(containerRequest.getHeaders()).thenReturn(headers);
      when(httpMetricsTagProvider.getRequestTags(headers)).thenReturn(Tags.of("extra", "true"));

      HttpCounterTagsProvider provider = new HttpCounterTagsProvider(httpMetricsTagProvider);
      Iterable<Tag> result = provider.httpRequestTags(requestEvent);

      assertThat(result).contains(Tag.of("extra", "true")).contains(Tag.of("error", "true"));
    }

    @Test
    public void noExtraProvider() {
      when(containerResponse.getStatus()).thenReturn(200);

      HttpCounterTagsProvider provider = new HttpCounterTagsProvider();
      Iterable<Tag> result = provider.httpRequestTags(requestEvent);

      assertThat(result).contains(Tag.of("error", "false"));
    }

    @Test
    public void noResponse() {
      when(requestEvent.getContainerResponse()).thenReturn(null);

      HttpCounterTagsProvider provider = new HttpCounterTagsProvider();
      Iterable<Tag> result = provider.httpRequestTags(requestEvent);

      assertThat(result).contains(Tag.of("error", "true"));
    }
  }

  @Nested
  class HttpLongRequestTags {

    @Test
    public void happyPath() {
      MultivaluedMap<String, String> headers = new MultivaluedStringMap();
      headers.putSingle("some", "header");

      when(containerRequest.getHeaders()).thenReturn(headers);
      when(httpMetricsTagProvider.getRequestTags(headers)).thenReturn(Tags.of("extra", "true"));

      HttpCounterTagsProvider provider = new HttpCounterTagsProvider(httpMetricsTagProvider);
      Iterable<Tag> result = provider.httpLongRequestTags(requestEvent);

      assertThat(result).contains(Tag.of("extra", "true"));
    }

    @Test
    public void noExtraProvider() {
      HttpCounterTagsProvider provider = new HttpCounterTagsProvider();
      Iterable<Tag> result = provider.httpLongRequestTags(requestEvent);

      assertThat(result).isEmpty();
    }
  }
}
