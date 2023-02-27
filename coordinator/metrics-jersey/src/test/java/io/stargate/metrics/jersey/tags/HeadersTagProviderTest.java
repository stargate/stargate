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
import java.util.Arrays;
import javax.ws.rs.core.MultivaluedMap;
import org.glassfish.jersey.internal.util.collection.MultivaluedStringMap;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class HeadersTagProviderTest {

  @Mock RequestEvent requestEvent;

  @Mock ContainerRequest containerRequest;

  @BeforeEach
  public void mockEvent() {
    lenient().when(requestEvent.getContainerRequest()).thenReturn(containerRequest);
  }

  @Nested
  class HttpRequestTags {

    @Test
    public void happyPath() {
      MultivaluedMap<String, String> headers = new MultivaluedStringMap();
      headers.putSingle("header1", "value1");
      headers.put("header2", Arrays.asList("value1", "value2"));
      when(containerRequest.getHeaders()).thenReturn(headers);

      HeadersTagProvider.Config config = HeadersTagProvider.Config.fromPropertyString("header1");
      HeadersTagProvider provider = new HeadersTagProvider(config);
      Iterable<Tag> result = provider.httpRequestTags(requestEvent);

      assertThat(result).containsOnly(Tag.of("header1", "value1"));
      assertThat(result).isEqualTo(provider.httpLongRequestTags(requestEvent));
    }

    @Test
    public void caseIrrelevant() {
      MultivaluedMap<String, String> headers = new MultivaluedStringMap();
      headers.putSingle("header1", "value1");
      headers.put("header2", Arrays.asList("value1", "value2"));
      when(containerRequest.getHeaders()).thenReturn(headers);

      HeadersTagProvider.Config config =
          HeadersTagProvider.Config.fromPropertyString("HEADER1,header2");
      HeadersTagProvider provider = new HeadersTagProvider(config);
      Iterable<Tag> result = provider.httpRequestTags(requestEvent);

      assertThat(result)
          .containsOnly(Tag.of("header1", "value1"), Tag.of("header2", "value1,value2"));
      assertThat(result).isEqualTo(provider.httpLongRequestTags(requestEvent));
    }

    @Test
    public void missingHeaderAsUnknown() {
      MultivaluedMap<String, String> headers = new MultivaluedStringMap();
      headers.putSingle("header1", "value1");
      headers.put("header2", Arrays.asList("value1", "value2"));
      when(containerRequest.getHeaders()).thenReturn(headers);

      HeadersTagProvider.Config config =
          HeadersTagProvider.Config.fromPropertyString("otherHeader");
      HeadersTagProvider provider = new HeadersTagProvider(config);
      Iterable<Tag> result = provider.httpRequestTags(requestEvent);

      assertThat(result).containsOnly(Tag.of("otherheader", "unknown"));
      assertThat(result).isEqualTo(provider.httpLongRequestTags(requestEvent));
    }

    @Test
    public void collectNothing() {
      MultivaluedMap<String, String> headers = new MultivaluedStringMap();
      headers.putSingle("header1", "value1");
      headers.put("header2", Arrays.asList("value1", "value2"));
      when(containerRequest.getHeaders()).thenReturn(headers);

      HeadersTagProvider.Config config = HeadersTagProvider.Config.fromPropertyString(null);
      HeadersTagProvider provider = new HeadersTagProvider(config);
      Iterable<Tag> result = provider.httpRequestTags(requestEvent);

      assertThat(result).isEmpty();
      assertThat(result).isEqualTo(provider.httpLongRequestTags(requestEvent));
    }

    @Test
    public void collectNothingEmptyConfigString() {
      MultivaluedMap<String, String> headers = new MultivaluedStringMap();
      headers.putSingle("header1", "value1");
      headers.put("header2", Arrays.asList("value1", "value2"));
      when(containerRequest.getHeaders()).thenReturn(headers);

      HeadersTagProvider.Config config = HeadersTagProvider.Config.fromPropertyString("");
      HeadersTagProvider provider = new HeadersTagProvider(config);
      Iterable<Tag> result = provider.httpRequestTags(requestEvent);

      assertThat(result).isEmpty();
      assertThat(result).isEqualTo(provider.httpLongRequestTags(requestEvent));
    }
  }
}
