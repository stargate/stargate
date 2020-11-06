/*
 * Copyright 2018-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.producer.kafka;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import io.stargate.config.store.api.ConfigStore;
import io.stargate.core.metrics.api.Metrics;
import java.util.Hashtable;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Filter;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceReference;

class KafkaProducerActivatorTest {

  @Test
  public void shouldStartIfBothServicesAreRegistered() throws InvalidSyntaxException {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    KafkaProducerActivator activator = new KafkaProducerActivator();
    mockFilterForBothServices(bundleContext);
    activator.start(bundleContext);

    // when
    activator.tracker.startIfAllRegistered(mock(ServiceReference.class), mockMetrics());
    activator.tracker.startIfAllRegistered(mock(ServiceReference.class), mock(ConfigStore.class));

    // then should not register service
    verify(bundleContext, times(1))
        .registerService(
            eq(KafkaCDCProducer.class.getName()),
            any(KafkaCDCProducer.class),
            eq(new Hashtable<>()));
    Assertions.assertThat(activator.started).isTrue();
  }

  @Test
  public void shouldNotStartIfOnlyConfigStoreServiceIsRegistered() throws InvalidSyntaxException {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    KafkaProducerActivator activator = new KafkaProducerActivator();
    mockFilterForBothServices(bundleContext);
    activator.start(bundleContext);

    // when
    activator.tracker.startIfAllRegistered(mock(ServiceReference.class), mock(ConfigStore.class));

    // then should not register service
    verify(bundleContext, times(0))
        .registerService(
            eq(KafkaCDCProducer.class.getName()),
            any(KafkaCDCProducer.class),
            eq(new Hashtable<>()));
    Assertions.assertThat(activator.started).isFalse();
  }

  @Test
  public void shouldNotStartIfOnlyMetricsServiceIsRegistered() throws InvalidSyntaxException {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    KafkaProducerActivator activator = new KafkaProducerActivator();
    mockFilterForBothServices(bundleContext);
    activator.start(bundleContext);

    // when
    activator.tracker.startIfAllRegistered(mock(ServiceReference.class), mockMetrics());

    // then should not register service
    verify(bundleContext, times(0))
        .registerService(
            eq(KafkaCDCProducer.class.getName()),
            any(KafkaCDCProducer.class),
            eq(new Hashtable<>()));
    Assertions.assertThat(activator.started).isFalse();
  }

  private Metrics mockMetrics() {
    Metrics metrics = mock(Metrics.class);
    when(metrics.getRegistry(eq(KafkaProducerActivator.KAFKA_CDC_METRICS_PREFIX)))
        .thenReturn(new MetricRegistry());
    return metrics;
  }

  private void mockFilterForBothServices(BundleContext bundleContext)
      throws InvalidSyntaxException {
    when(bundleContext.createFilter(
            String.format(
                "(|(objectClass=%s)(objectClass=%s))",
                Metrics.class.getName(), ConfigStore.class.getName())))
        .thenReturn(mock(Filter.class));
  }
}
