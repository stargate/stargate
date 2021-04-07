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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.health;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import io.stargate.core.metrics.api.MetricsScraper;
import javax.ws.rs.core.Response;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PrometheusResourceTest {

  @InjectMocks PrometheusResource prometheusResource;

  @Mock MetricsScraper scraper;

  @Nested
  class PrometheusEndpoint {

    @Test
    public void happyPath() {
      String metrics = "my_metric=1";
      when(scraper.scrape()).thenReturn(metrics);

      Response response = prometheusResource.prometheusEndpoint();

      assertThat(response.getStatus()).isEqualTo(200);
      assertThat(response.getEntity()).isEqualTo(metrics);
    }
  }
}
