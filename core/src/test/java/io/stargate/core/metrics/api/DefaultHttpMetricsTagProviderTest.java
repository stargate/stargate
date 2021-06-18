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

package io.stargate.core.metrics.api;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class DefaultHttpMetricsTagProviderTest {

  @Nested
  class GetRequestTags {

    @BeforeEach
    @AfterEach
    public void clearProperty() {
      System.clearProperty("stargate.metrics.http_server_requests_header_tags");
    }

    @Test
    public void happyPath() {
      System.setProperty("stargate.metrics.http_server_requests_header_tags", "header1");
      Map<String, List<String>> headers = new HashMap<>();
      headers.put("header1", Collections.singletonList("value1"));
      headers.put("header2", Arrays.asList("value1", "value2"));

      DefaultHttpMetricsTagProvider provider = new DefaultHttpMetricsTagProvider();
      Tags result = provider.getRequestTags(headers);

      assertThat(result).containsOnly(Tag.of("header1", "value1"));
    }

    @Test
    public void caseIrrelevant() {
      System.setProperty("stargate.metrics.http_server_requests_header_tags", "HEADER1,header2");
      Map<String, List<String>> headers = new HashMap<>();
      headers.put("header1", Collections.singletonList("value1"));
      headers.put("Header2", Arrays.asList("value1", "value2"));

      DefaultHttpMetricsTagProvider provider = new DefaultHttpMetricsTagProvider();
      Tags result = provider.getRequestTags(headers);

      assertThat(result)
          .containsOnly(Tag.of("header1", "value1"), Tag.of("Header2", "value1,value2"));
    }

    @Test
    public void collectNothing() {
      Map<String, List<String>> headers = new HashMap<>();
      headers.put("header1", Collections.singletonList("value1"));
      headers.put("header2", Arrays.asList("value1", "value2"));

      DefaultHttpMetricsTagProvider provider = new DefaultHttpMetricsTagProvider();
      Tags result = provider.getRequestTags(headers);

      assertThat(result).isEmpty();
    }
  }
}
