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
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.stargate.core.metrics.api.HttpMetricsTagProvider;
import java.util.Collections;
import javax.ws.rs.core.MultivaluedMap;
import org.glassfish.jersey.internal.util.collection.MultivaluedStringMap;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.ContainerResponse;
import org.glassfish.jersey.server.ExtendedUriInfo;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.uri.UriTemplate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class HttpMeterTagsProviderTest {

  @Mock HttpMetricsTagProvider httpMetricsTagProvider;

  @Mock RequestEvent requestEvent;

  @Mock ContainerResponse containerResponse;

  @Mock ContainerRequest containerRequest;

  @Mock ExtendedUriInfo extendedUriInfo;

  @BeforeEach
  public void mockEvent() {
    when(requestEvent.getContainerRequest()).thenReturn(containerRequest);
    when(requestEvent.getContainerResponse()).thenReturn(containerResponse);
    when(requestEvent.getUriInfo()).thenReturn(extendedUriInfo);
  }

  @Nested
  class HttpRequestTags {

    @Test
    public void happyPath() {
      MultivaluedMap<String, String> headers = new MultivaluedStringMap();
      headers.putSingle("some", "header");

      when(extendedUriInfo.getMatchedTemplates())
          .thenReturn(Collections.singletonList(new UriTemplate("/target/uri")));
      when(extendedUriInfo.getBaseUri()).thenReturn(UriTemplate.normalize("http://localhost/base"));
      when(containerRequest.getMethod()).thenReturn("GET");
      when(containerRequest.getHeaders()).thenReturn(headers);
      when(containerResponse.getStatus()).thenReturn(200);
      when(httpMetricsTagProvider.getRequestTags(headers)).thenReturn(Tags.of("extra", "true"));

      HttpMeterTagsProvider provider = new HttpMeterTagsProvider(httpMetricsTagProvider);
      Iterable<Tag> result = provider.httpRequestTags(requestEvent);

      assertThat(result)
          .contains(Tag.of("extra", "true"))
          .contains(Tag.of("method", "GET"))
          .contains(Tag.of("status", "200"))
          .contains(Tag.of("uri", "/base/target/uri"));
    }

    @Test
    public void extraEmpty() {
      MultivaluedMap<String, String> headers = new MultivaluedStringMap();
      headers.putSingle("some", "header");

      when(extendedUriInfo.getMatchedTemplates())
          .thenReturn(Collections.singletonList(new UriTemplate("/target/uri")));
      when(extendedUriInfo.getBaseUri()).thenReturn(UriTemplate.normalize("http://localhost/base"));
      when(containerRequest.getMethod()).thenReturn("POST");
      when(containerRequest.getHeaders()).thenReturn(headers);
      when(containerResponse.getStatus()).thenReturn(201);
      when(httpMetricsTagProvider.getRequestTags(headers)).thenReturn(Tags.empty());

      HttpMeterTagsProvider provider = new HttpMeterTagsProvider(httpMetricsTagProvider);
      Iterable<Tag> result = provider.httpRequestTags(requestEvent);

      assertThat(result)
          .contains(Tag.of("method", "POST"))
          .contains(Tag.of("status", "201"))
          .contains(Tag.of("uri", "/base/target/uri"));
    }

    @Test
    public void noExtraProvider() {
      when(extendedUriInfo.getMatchedTemplates())
          .thenReturn(Collections.singletonList(new UriTemplate("/target/uri")));
      when(extendedUriInfo.getBaseUri()).thenReturn(UriTemplate.normalize("http://localhost/base"));
      when(containerRequest.getMethod()).thenReturn("GET");
      when(containerResponse.getStatus()).thenReturn(200);

      HttpMeterTagsProvider provider = new HttpMeterTagsProvider();
      Iterable<Tag> result = provider.httpRequestTags(requestEvent);

      assertThat(result)
          .contains(Tag.of("method", "GET"))
          .contains(Tag.of("status", "200"))
          .contains(Tag.of("uri", "/base/target/uri"));
    }
  }

  @Nested
  class HttpLongRequestTags {

    @Test
    public void happyPath() {
      MultivaluedMap<String, String> headers = new MultivaluedStringMap();
      headers.putSingle("some", "header");

      when(extendedUriInfo.getMatchedTemplates())
          .thenReturn(Collections.singletonList(new UriTemplate("/target/uri")));
      when(extendedUriInfo.getBaseUri()).thenReturn(UriTemplate.normalize("http://localhost/base"));
      when(containerRequest.getMethod()).thenReturn("GET");
      when(containerRequest.getHeaders()).thenReturn(headers);
      when(containerResponse.getStatus()).thenReturn(200);
      when(httpMetricsTagProvider.getRequestTags(headers)).thenReturn(Tags.of("extra", "true"));

      HttpMeterTagsProvider provider = new HttpMeterTagsProvider(httpMetricsTagProvider);
      Iterable<Tag> result = provider.httpLongRequestTags(requestEvent);

      assertThat(result)
          .contains(Tag.of("extra", "true"))
          .contains(Tag.of("method", "GET"))
          .contains(Tag.of("uri", "/base/target/uri"));
    }

    @Test
    public void noExtraProvider() {
      when(extendedUriInfo.getMatchedTemplates())
          .thenReturn(Collections.singletonList(new UriTemplate("/target/uri")));
      when(extendedUriInfo.getBaseUri()).thenReturn(UriTemplate.normalize("http://localhost/base"));
      when(containerRequest.getMethod()).thenReturn("GET");
      when(containerResponse.getStatus()).thenReturn(200);

      HttpMeterTagsProvider provider = new HttpMeterTagsProvider();
      Iterable<Tag> result = provider.httpLongRequestTags(requestEvent);

      assertThat(result)
          .contains(Tag.of("method", "GET"))
          .contains(Tag.of("uri", "/base/target/uri"));
    }
  }
}
