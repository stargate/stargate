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
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.Tag;
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
  }

  @Nested
  class HttpRequestTags {

    @Test
    public void matchSpecific() {
      MultivaluedHashMap<String, String> paramMap = new MultivaluedHashMap<>();
      paramMap.putSingle("k1", "v1");
      paramMap.putSingle("k2", "v2");
      when(extendedUriInfo.getPathParameters(true)).thenReturn(paramMap);

      PathParametersTagsProvider.Config config =
          PathParametersTagsProvider.Config.fromPropertyValue("k2");
      PathParametersTagsProvider provider = new PathParametersTagsProvider(config);
      Iterable<Tag> result = provider.httpRequestTags(requestEvent);

      assertThat(result).hasSize(1).contains(Tag.of("k2", "v2"));
    }

    @Test
    public void matchSpecificMultiple() {
      MultivaluedHashMap<String, String> paramMap = new MultivaluedHashMap<>();
      paramMap.putSingle("k1", "v1");
      paramMap.putSingle("k2", "v2");
      when(extendedUriInfo.getPathParameters(true)).thenReturn(paramMap);

      PathParametersTagsProvider.Config config =
          PathParametersTagsProvider.Config.fromPropertyValue("k1,k2");
      PathParametersTagsProvider provider = new PathParametersTagsProvider(config);
      Iterable<Tag> result = provider.httpRequestTags(requestEvent);

      assertThat(result).hasSize(2).contains(Tag.of("k1", "v1")).contains(Tag.of("k2", "v2"));
    }

    @Test
    public void handleMissing() {
      MultivaluedHashMap<String, String> paramMap = new MultivaluedHashMap<>();
      paramMap.putSingle("k1", "v1");
      paramMap.putSingle("k2", "v2");
      when(extendedUriInfo.getPathParameters(true)).thenReturn(paramMap);

      PathParametersTagsProvider.Config config =
          PathParametersTagsProvider.Config.fromPropertyValue("k3");
      PathParametersTagsProvider provider = new PathParametersTagsProvider(config);
      Iterable<Tag> result = provider.httpRequestTags(requestEvent);

      assertThat(result).hasSize(1).contains(Tag.of("k3", "unknown"));
    }

    @Test
    public void matchNothing() {
      PathParametersTagsProvider.Config config =
          PathParametersTagsProvider.Config.fromPropertyValue(null);
      PathParametersTagsProvider provider = new PathParametersTagsProvider(config);
      Iterable<Tag> result = provider.httpRequestTags(requestEvent);

      assertThat(result).isEmpty();
      verifyNoInteractions(requestEvent);
    }

    @Test
    public void matchNothingEmptyPropString() {
      PathParametersTagsProvider.Config config =
          PathParametersTagsProvider.Config.fromPropertyValue("");
      PathParametersTagsProvider provider = new PathParametersTagsProvider(config);
      Iterable<Tag> result = provider.httpRequestTags(requestEvent);

      assertThat(result).isEmpty();
      verifyNoInteractions(requestEvent);
    }
  }
}
