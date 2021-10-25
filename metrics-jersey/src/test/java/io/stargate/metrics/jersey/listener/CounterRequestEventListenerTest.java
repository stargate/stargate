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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.jersey2.server.JerseyTagsProvider;
import java.util.Collection;
import org.apache.commons.lang3.RandomStringUtils;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CounterRequestEventListenerTest {

  private static final String METRIC_NAME = RandomStringUtils.randomAlphanumeric(8);

  CounterRequestEventListener listener;

  MeterRegistry meterRegistry;

  @Mock JerseyTagsProvider tagsProvider;

  @Mock RequestEvent requestEvent;

  @BeforeEach
  public void init() {
    meterRegistry = new SimpleMeterRegistry();
    listener = new CounterRequestEventListener(meterRegistry, tagsProvider, METRIC_NAME);
  }

  @Nested
  class OnEvent {

    @Test
    public void happyPath() {
      Tags tags = Tags.of("one", "two");
      when(tagsProvider.httpRequestTags(requestEvent)).thenReturn(tags);
      when(requestEvent.getType()).thenReturn(RequestEvent.Type.FINISHED);

      listener.onEvent(requestEvent);

      Counter counter = meterRegistry.find(METRIC_NAME).tags(tags).counter();
      assertThat(counter.count()).isEqualTo(1d);
      verify(tagsProvider).httpRequestTags(requestEvent);
      verifyNoMoreInteractions(tagsProvider);
    }

    @Test
    public void otherEventType() {
      when(requestEvent.getType()).thenReturn(RequestEvent.Type.ON_EXCEPTION);

      listener.onEvent(requestEvent);

      Collection<Counter> counters = meterRegistry.find(METRIC_NAME).counters();

      assertThat(counters).isEmpty();
      verifyNoMoreInteractions(tagsProvider);
    }
  }
}
