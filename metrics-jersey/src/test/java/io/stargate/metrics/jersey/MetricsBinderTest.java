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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.dropwizard.jersey.setup.JerseyEnvironment;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.jersey2.server.MetricsApplicationEventListener;
import io.stargate.core.metrics.api.HttpMetricsTagProvider;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.metrics.jersey.config.MetricsListenerConfig;
import io.stargate.metrics.jersey.listener.CounterApplicationEventListener;
import java.util.Collections;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MetricsBinderTest {

  private static final String MODULE = RandomStringUtils.randomAlphanumeric(8);

  MetricsBinder binder;

  @Mock Metrics metrics;

  @Mock HttpMetricsTagProvider httpMetricsTagProvider;

  @Mock MetricsListenerConfig meterListenerConfig;

  @Mock MetricsListenerConfig counterListenerConfig;

  @Mock JerseyEnvironment jerseyEnvironment;

  @Mock MeterRegistry meterRegistry;

  @BeforeEach
  public void init() {
    binder =
        new MetricsBinder(
            metrics,
            httpMetricsTagProvider,
            MODULE,
            Collections.emptyList(),
            meterListenerConfig,
            counterListenerConfig);
  }

  @Nested
  class Register {

    @Captor ArgumentCaptor<Object> registerComponentCaptor;

    @Test
    public void happyPath() {
      when(metrics.getMeterRegistry()).thenReturn(meterRegistry);
      when(meterListenerConfig.isEnabled()).thenReturn(true);
      when(counterListenerConfig.isEnabled()).thenReturn(true);

      binder.register(jerseyEnvironment);

      verify(jerseyEnvironment, times(2)).register(registerComponentCaptor.capture());
      verify(metrics, times(2)).getMeterRegistry();
      verify(metrics, times(2)).tagsForModule(MODULE);
      verify(metrics, times(2)).tagsForModule(MODULE + "-other");
      verifyNoMoreInteractions(metrics, httpMetricsTagProvider);
      assertThat(registerComponentCaptor.getAllValues())
          .hasSize(2)
          .anySatisfy(
              c -> {
                assertThat(c)
                    .isInstanceOf(MetricsApplicationEventListener.class)
                    .hasFieldOrPropertyWithValue("meterRegistry", meterRegistry)
                    .hasFieldOrPropertyWithValue("metricName", "http.server.requests");
              })
          .anySatisfy(
              c -> {
                assertThat(c)
                    .isInstanceOf(CounterApplicationEventListener.class)
                    .hasFieldOrPropertyWithValue("meterRegistry", meterRegistry)
                    .hasFieldOrPropertyWithValue("metricName", "http.server.requests.counter");
              });
    }

    @Test
    public void ignored() {
      when(meterListenerConfig.isEnabled()).thenReturn(false);
      when(counterListenerConfig.isEnabled()).thenReturn(false);

      binder.register(jerseyEnvironment);

      verifyNoMoreInteractions(jerseyEnvironment, metrics, httpMetricsTagProvider);
    }
  }
}
