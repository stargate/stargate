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

package io.stargate.metrics.jersey.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.jersey2.server.JerseyTagsProvider;
import org.glassfish.jersey.server.monitoring.ApplicationEvent;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.server.monitoring.RequestEventListener;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CounterApplicationEventListenerTest {

  @Mock private MeterRegistry registry;

  @Mock private JerseyTagsProvider tagsProvider;

  @Mock private ApplicationEvent applicationEvent;

  @Mock private RequestEvent requestEvent;

  @Nested
  class OnEvent {

    @Test
    public void happyPath() {
      CounterApplicationEventListener listener =
          new CounterApplicationEventListener(registry, tagsProvider, "module");

      listener.onEvent(applicationEvent);

      verifyNoMoreInteractions(registry, tagsProvider);
    }
  }

  @Nested
  class onRequest {

    @Test
    public void happyPath() {
      CounterApplicationEventListener listener =
          new CounterApplicationEventListener(registry, tagsProvider, "module");

      RequestEventListener result = listener.onRequest(requestEvent);

      assertThat(result)
          .isInstanceOf(CounterRequestEventListener.class)
          .hasFieldOrPropertyWithValue("registry", registry)
          .hasFieldOrPropertyWithValue("tagsProvider", tagsProvider)
          .hasFieldOrPropertyWithValue("metricName", "module");
      verifyNoMoreInteractions(registry, tagsProvider);
    }
  }
}
