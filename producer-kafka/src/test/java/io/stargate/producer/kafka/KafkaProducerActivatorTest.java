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
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.db.cdc.CDCProducer;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.osgi.framework.BundleContext;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceReference;

class KafkaProducerActivatorTest {

  @Test
  public void shouldNotStartAndRegisterListenerWhenMetricsServiceNotProvided()
      throws InvalidSyntaxException {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    KafkaProducerActivator kafkaProducerActivator = new KafkaProducerActivator();

    // when
    kafkaProducerActivator.start(bundleContext);

    // then register service listener and not start
    verify(bundleContext, times(1))
        .addServiceListener(any(), eq(String.format("(objectClass=%s)", Metrics.class.getName())));
    assertThat(kafkaProducerActivator.started).isFalse();
  }

  @Test
  public void shouldStartAndRegisterServiceAndNotRegisterListenerWhenMetricsServiceProvided()
      throws InvalidSyntaxException {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    KafkaProducerActivator kafkaProducerActivator = new KafkaProducerActivator();
    ServiceReference<Metrics> serviceReference = mock(ServiceReference.class);
    Metrics metrics = mock(Metrics.class);
    doReturn(serviceReference).when(bundleContext).getServiceReference(Metrics.class.getName());
    when(bundleContext.getService(serviceReference)).thenReturn(metrics);
    when(metrics.getRegistry(any())).thenReturn(new MetricRegistry());

    // when
    kafkaProducerActivator.start(bundleContext);

    // then register service and start
    verify(bundleContext, times(1))
        .registerService(eq(CDCProducer.class), any(CDCProducer.class), eq(null));
    assertThat(kafkaProducerActivator.started).isTrue();
    // not register listener
    verify(bundleContext, times(0))
        .addServiceListener(any(), eq(String.format("(objectClass=%s)", Metrics.class.getName())));
  }

  @Test
  public void shouldRegisterServiceOnlyOnceEvenIfStartIsCalledSecondTime()
      throws InvalidSyntaxException {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    KafkaProducerActivator kafkaProducerActivator = new KafkaProducerActivator();
    ServiceReference<Metrics> serviceReference = mock(ServiceReference.class);
    Metrics metrics = mock(Metrics.class);
    doReturn(serviceReference).when(bundleContext).getServiceReference(Metrics.class.getName());
    when(bundleContext.getService(serviceReference)).thenReturn(metrics);
    when(metrics.getRegistry(any())).thenReturn(new MetricRegistry());

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
}
