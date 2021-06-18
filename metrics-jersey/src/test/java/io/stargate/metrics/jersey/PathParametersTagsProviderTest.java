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

package io.stargate.metrics.jersey;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.Tag;
import java.util.Arrays;
import javax.ws.rs.core.MultivaluedHashMap;
import org.glassfish.jersey.server.ExtendedUriInfo;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PathParametersTagsProviderTest {

  @Mock RequestEvent requestEvent;

  @Mock ExtendedUriInfo extendedUriInfo;

  @BeforeEach
  public void mockEvent() {
    lenient().when(requestEvent.getUriInfo()).thenReturn(extendedUriInfo);
    System.clearProperty("stargate.metrics.http_server_requests_path_param_tags");
  }

  @Nested
  class HttpRequestTags {

    @Test
    public void matchSpecific() {
      System.setProperty("stargate.metrics.http_server_requests_path_param_tags", "k2");
      MultivaluedHashMap<String, String> paramMap = new MultivaluedHashMap<>();
      paramMap.putSingle("k1", "v1");
      paramMap.putSingle("k2", "v2");
      when(extendedUriInfo.getPathParameters(true)).thenReturn(paramMap);

      PathParametersTagsProvider provider = new PathParametersTagsProvider();
      Iterable<Tag> result = provider.httpRequestTags(requestEvent);

      assertThat(result).hasSize(1).contains(Tag.of("k2", "v2"));
    }

    @Test
    public void matchSpecificMultiple() {
      System.setProperty("stargate.metrics.http_server_requests_path_param_tags", "k1,k2");
      MultivaluedHashMap<String, String> paramMap = new MultivaluedHashMap<>();
      paramMap.putSingle("k1", "v1");
      paramMap.putSingle("k2", "v2");
      when(extendedUriInfo.getPathParameters(true)).thenReturn(paramMap);

      PathParametersTagsProvider provider = new PathParametersTagsProvider();
      Iterable<Tag> result = provider.httpRequestTags(requestEvent);

      assertThat(result).hasSize(2).contains(Tag.of("k1", "v1")).contains(Tag.of("k2", "v2"));
    }

    @Test
    public void matchAll() {
      System.setProperty("stargate.metrics.http_server_requests_path_param_tags", "*");
      MultivaluedHashMap<String, String> paramMap = new MultivaluedHashMap<>();
      paramMap.putSingle("k1", "v1");
      paramMap.put("k2", Arrays.asList("v2", "v3"));
      when(extendedUriInfo.getPathParameters(true)).thenReturn(paramMap);

      PathParametersTagsProvider provider = new PathParametersTagsProvider();
      Iterable<Tag> result = provider.httpRequestTags(requestEvent);

      assertThat(result).hasSize(2).contains(Tag.of("k1", "v1")).contains(Tag.of("k2", "v2,v3"));
    }

    @Test
    public void matchAllButEmpty() {
      System.setProperty("stargate.metrics.http_server_requests_path_param_tags", "*");
      MultivaluedHashMap<String, String> paramMap = new MultivaluedHashMap<>();
      when(extendedUriInfo.getPathParameters(true)).thenReturn(paramMap);

      PathParametersTagsProvider provider = new PathParametersTagsProvider();
      Iterable<Tag> result = provider.httpRequestTags(requestEvent);

      assertThat(result).isEmpty();
    }

    @Test
    public void matchNothing() {
      PathParametersTagsProvider provider = new PathParametersTagsProvider();
      Iterable<Tag> result = provider.httpRequestTags(requestEvent);

      assertThat(result).isEmpty();
      verifyNoInteractions(requestEvent);
    }
  }
}
