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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import io.stargate.config.store.api.ConfigStore;
import io.stargate.core.BundleUtils;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.db.cdc.CDCProducer;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.osgi.framework.BundleContext;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceEvent;
import org.osgi.framework.ServiceReference;

class KafkaProducerActivatorTest {

  @Test
  public void shouldNotStartAndRegisterListenersWhenMetricsAndConfigStoreServiceNotProvided()
      throws InvalidSyntaxException {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    KafkaProducerActivator kafkaProducerActivator = new KafkaProducerActivator();

    // when
    kafkaProducerActivator.start(bundleContext);

    // then register service listeners for both config-store and metrics
    verify(bundleContext, times(1))
        .addServiceListener(any(), eq(String.format("(objectClass=%s)", Metrics.class.getName())));
    verify(bundleContext, times(1))
        .addServiceListener(
            any(), eq(String.format("(objectClass=%s)", ConfigStore.class.getName())));
    assertThat(kafkaProducerActivator.started).isFalse();
  }

  @Test
  public void shouldNotStartAndRegisterListenerWhenMetricsProvidedButConfigStoreServiceNotProvided()
      throws InvalidSyntaxException {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    KafkaProducerActivator kafkaProducerActivator = new KafkaProducerActivator();
    mockMetrics(bundleContext);

    // when
    kafkaProducerActivator.start(bundleContext);

    // then register for config-store because it was not provided
    verify(bundleContext, times(0))
        .addServiceListener(any(), eq(String.format("(objectClass=%s)", Metrics.class.getName())));
    verify(bundleContext, times(1))
        .addServiceListener(
            any(), eq(String.format("(objectClass=%s)", ConfigStore.class.getName())));
    assertThat(kafkaProducerActivator.started).isFalse();
  }

  @Test
  public void shouldNotStartAndRegisterListenerWhenMetricsNotProvidedButConfigStoreServiceProvided()
      throws InvalidSyntaxException {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    KafkaProducerActivator kafkaProducerActivator = new KafkaProducerActivator();
    mockConfigStore(bundleContext);

    // when
    kafkaProducerActivator.start(bundleContext);

    // then register for metrics because it was not provided
    verify(bundleContext, times(1))
        .addServiceListener(any(), eq(String.format("(objectClass=%s)", Metrics.class.getName())));
    verify(bundleContext, times(0))
        .addServiceListener(
            any(), eq(String.format("(objectClass=%s)", ConfigStore.class.getName())));
    assertThat(kafkaProducerActivator.started).isFalse();
  }

  @Test
  public void
      shouldStartAndRegisterServiceAndNotRegisterListenersWhenBothMetricsAndConfigStoreServiceProvided()
          throws InvalidSyntaxException {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    KafkaProducerActivator kafkaProducerActivator = new KafkaProducerActivator();

    // metrics service provided
    mockMetrics(bundleContext);

    // config-store service provided
    mockConfigStore(bundleContext);

    // when
    kafkaProducerActivator.start(bundleContext);

    // then register service and start
    verify(bundleContext, times(1))
        .registerService(eq(CDCProducer.class), any(CDCProducer.class), eq(null));
    assertThat(kafkaProducerActivator.started).isTrue();
    // not register listeners
    verify(bundleContext, times(0))
        .addServiceListener(any(), eq(String.format("(objectClass=%s)", Metrics.class.getName())));
    verify(bundleContext, times(0))
        .addServiceListener(
            any(), eq(String.format("(objectClass=%s)", ConfigStore.class.getName())));
  }

  @Test
  public void shouldRegisterServiceOnlyOnceEvenIfStartIsCalledSecondTime()
      throws InvalidSyntaxException {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    KafkaProducerActivator kafkaProducerActivator = new KafkaProducerActivator();
    // metrics service provided
    mockMetrics(bundleContext);

    // config-store service provided
    mockConfigStore(bundleContext);

    // when
    kafkaProducerActivator.start(bundleContext);

    // then register service and start
    verify(bundleContext, times(1))
        .registerService(eq(CDCProducer.class), any(CDCProducer.class), eq(null));
    assertThat(kafkaProducerActivator.started).isTrue();

    // when start 2nd time
    Mockito.reset(bundleContext); // reset the invocations counter
    kafkaProducerActivator.start(bundleContext);

    // then should not register service and remain started
    verify(bundleContext, times(0))
        .registerService(eq(CDCProducer.class), any(CDCProducer.class), eq(null));
    assertThat(kafkaProducerActivator.started).isTrue();
  }

  @Test
  public void shouldNotStartIfOnlyMetricsServiceIsRegistered() throws InvalidSyntaxException {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    KafkaProducerActivator kafkaProducerActivator = new KafkaProducerActivator();
    kafkaProducerActivator.start(bundleContext);
    ServiceEvent metricsServiceEvent = mock(ServiceEvent.class);
    try (MockedStatic<BundleUtils> bundleUtilsMock = mockStatic(BundleUtils.class)) {
      mockMetricsServiceNotification(bundleContext, metricsServiceEvent, bundleUtilsMock);

      // when
      kafkaProducerActivator.serviceChanged(metricsServiceEvent);

      // then should not register service
      verify(bundleContext, times(0))
          .registerService(eq(CDCProducer.class), any(CDCProducer.class), eq(null));
      assertThat(kafkaProducerActivator.started).isFalse();
    }
  }

  @Test
  public void shouldNotStartIfOnlyConfigStoreServiceIsRegistered() throws InvalidSyntaxException {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    KafkaProducerActivator kafkaProducerActivator = new KafkaProducerActivator();
    kafkaProducerActivator.start(bundleContext);
    ConfigStore configStore = mock(ConfigStore.class);
    ServiceEvent configStoreServiceEvent = mock(ServiceEvent.class);
    try (MockedStatic<BundleUtils> bundleUtilsMock = mockStatic(BundleUtils.class)) {
      // simulate configStore registration event
      bundleUtilsMock
          .when(
              () ->
                  BundleUtils.getRegisteredService(
                      eq(bundleContext), eq(configStoreServiceEvent), eq(ConfigStore.class)))
          .thenReturn(configStore);

      // when
      kafkaProducerActivator.serviceChanged(configStoreServiceEvent);

      // then should not register service
      verify(bundleContext, times(0))
          .registerService(eq(CDCProducer.class), any(CDCProducer.class), eq(null));
      assertThat(kafkaProducerActivator.started).isFalse();
    }
  }

  @Test
  public void shouldStartIfMetricsServiceAndConfigStoreAreRegistered()
      throws InvalidSyntaxException {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    KafkaProducerActivator kafkaProducerActivator = new KafkaProducerActivator();
    kafkaProducerActivator.start(bundleContext);
    ServiceEvent metricsServiceEvent = mock(ServiceEvent.class);
    ServiceEvent configStoreServiceEvent = mock(ServiceEvent.class);

    try (MockedStatic<BundleUtils> bundleUtilsMock = mockStatic(BundleUtils.class)) {
      mockMetricsServiceNotification(bundleContext, metricsServiceEvent, bundleUtilsMock);
      mockConfigStoreServiceNotification(bundleContext, configStoreServiceEvent, bundleUtilsMock);

      // when
      kafkaProducerActivator.serviceChanged(metricsServiceEvent);
      kafkaProducerActivator.serviceChanged(configStoreServiceEvent);

      // then should not register service
      verify(bundleContext, times(1))
          .registerService(eq(CDCProducer.class), any(CDCProducer.class), eq(null));
      assertThat(kafkaProducerActivator.started).isTrue();
    }
  }

  @SuppressWarnings("unchecked")
  private void mockMetrics(BundleContext bundleContext) {
    ServiceReference<Metrics> metricsServiceReference = mock(ServiceReference.class);
    Metrics metrics = mock(Metrics.class);
    doReturn(metricsServiceReference)
        .when(bundleContext)
        .getServiceReference(Metrics.class.getName());
    when(bundleContext.getService(metricsServiceReference)).thenReturn(metrics);
    when(metrics.getRegistry(any())).thenReturn(new MetricRegistry());
  }

  @SuppressWarnings("unchecked")
  private void mockConfigStore(BundleContext bundleContext) {
    ServiceReference<ConfigStore> configStoreServiceReference = mock(ServiceReference.class);
    ConfigStore configStore = mock(ConfigStore.class);
    doReturn(configStoreServiceReference)
        .when(bundleContext)
        .getServiceReference(ConfigStore.class.getName());
    when(bundleContext.getService(configStoreServiceReference)).thenReturn(configStore);
  }

  private void mockMetricsServiceNotification(
      BundleContext bundleContext,
      ServiceEvent metricsServiceEvent,
      MockedStatic<BundleUtils> bundleUtilsMock) {
    Metrics metrics = mock(Metrics.class);
    when(metrics.getRegistry(any())).thenReturn(new MetricRegistry());
    // simulate Metrics registration event
    bundleUtilsMock
        .when(
            () ->
                BundleUtils.getRegisteredService(
                    eq(bundleContext), eq(metricsServiceEvent), eq(Metrics.class)))
        .thenReturn(metrics);
  }

  private void mockConfigStoreServiceNotification(
      BundleContext bundleContext,
      ServiceEvent configStoreServiceEvent,
      MockedStatic<BundleUtils> bundleUtilsMock) {
    ConfigStore configStore = mock(ConfigStore.class);
    // simulate configStore registration event
    bundleUtilsMock
        .when(
            () ->
                BundleUtils.getRegisteredService(
                    eq(bundleContext), eq(configStoreServiceEvent), eq(ConfigStore.class)))
        .thenReturn(configStore);
  }
}
