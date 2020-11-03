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
package io.stargate.core.activator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.stargate.core.BundleUtils;
import java.util.Hashtable;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.osgi.framework.BundleContext;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceEvent;
import org.osgi.framework.ServiceReference;

class BaseActivatorTest {

  private static Hashtable<String, String> EXPECTED_PROPERTIES = new Hashtable<>();

  static {
    EXPECTED_PROPERTIES.put("Identifier", "id_1");
  }

  @Test
  public void shouldNotStartAndRegisterListenersWhenBothDependentServicesAreNotProvided()
      throws InvalidSyntaxException {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    TestServiceActivator activator = new TestServiceActivator();

    // when
    activator.start(bundleContext);

    // then register service listeners for both dependant services
    verify(bundleContext, times(1))
        .addServiceListener(
            any(), eq(String.format("(objectClass=%s)", DependentService1.class.getName())));
    verify(bundleContext, times(1))
        .addServiceListener(
            any(), eq(String.format("(objectClass=%s)", DependentService2.class.getName())));
    assertThat(activator.started).isFalse();
  }

  @Test
  public void shouldNotStartAndRegisterListenerWhenFirstProvidedButSecondServiceNotProvided()
      throws InvalidSyntaxException {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    TestServiceActivator activator = new TestServiceActivator();
    mockService1(bundleContext);

    // when
    activator.start(bundleContext);

    // then register for DependentService2 because is not provided
    verify(bundleContext, times(0))
        .addServiceListener(
            any(), eq(String.format("(objectClass=%s)", DependentService1.class.getName())));
    verify(bundleContext, times(1))
        .addServiceListener(
            any(), eq(String.format("(objectClass=%s)", DependentService2.class.getName())));
    assertThat(activator.started).isFalse();
  }

  @Test
  public void shouldNotStartAndRegisterListenerWhenFirstNotProvidedAndSecondServiceProvided()
      throws InvalidSyntaxException {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    TestServiceActivator activator = new TestServiceActivator();
    mockService2(bundleContext);

    // when
    activator.start(bundleContext);

    // then register for DependentService2 because is not provided
    verify(bundleContext, times(1))
        .addServiceListener(
            any(), eq(String.format("(objectClass=%s)", DependentService1.class.getName())));
    verify(bundleContext, times(0))
        .addServiceListener(
            any(), eq(String.format("(objectClass=%s)", DependentService2.class.getName())));
    assertThat(activator.started).isFalse();
  }

  @Test
  public void shouldStartAndRegisterServiceAndNotRegisterListenersWhenBotServicesProvided()
      throws InvalidSyntaxException {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    TestServiceActivator activator = new TestServiceActivator();

    // both services provided
    mockService1(bundleContext);
    mockService2(bundleContext);

    // when
    activator.start(bundleContext);

    // then register service and start
    verify(bundleContext, times(1))
        .registerService(
            eq(TestService.class.getName()), any(TestService.class), eq(EXPECTED_PROPERTIES));
    assertThat(activator.started).isTrue();
    // not register listeners
    verify(bundleContext, times(0))
        .addServiceListener(
            any(), eq(String.format("(objectClass=%s)", DependentService1.class.getName())));
    verify(bundleContext, times(0))
        .addServiceListener(
            any(), eq(String.format("(objectClass=%s)", DependentService2.class.getName())));
  }

  @Test
  public void shouldRegisterServiceOnlyOnceEvenIfStartIsCalledSecondTime()
      throws InvalidSyntaxException {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    TestServiceActivator activator = new TestServiceActivator();

    // both services provided
    mockService1(bundleContext);
    mockService2(bundleContext);

    // when
    activator.start(bundleContext);

    // then register service and start
    verify(bundleContext, times(1))
        .registerService(
            eq(TestService.class.getName()), any(TestService.class), eq(EXPECTED_PROPERTIES));
    assertThat(activator.started).isTrue();

    // when start 2nd time
    Mockito.reset(bundleContext); // reset the invocations counter
    activator.start(bundleContext);

    // then should not register service and remain started
    verify(bundleContext, times(0))
        .registerService(
            eq(TestService.class.getName()), any(TestService.class), eq(EXPECTED_PROPERTIES));
    assertThat(activator.started).isTrue();
  }

  @Test
  public void shouldNotStartIfOnlyFirstServiceIsRegistered() throws InvalidSyntaxException {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    TestServiceActivator activator = new TestServiceActivator();
    activator.start(bundleContext);
    ServiceEvent service1ServiceEvent = mock(ServiceEvent.class);
    try (MockedStatic<BundleUtils> bundleUtilsMock = mockStatic(BundleUtils.class)) {
      // simulate registration event
      mockService1Notification(bundleContext, service1ServiceEvent, bundleUtilsMock);

      // when
      activator.serviceChanged(service1ServiceEvent);

      // then should not register service
      verify(bundleContext, times(0))
          .registerService(
              eq(TestService.class.getName()), any(TestService.class), eq(EXPECTED_PROPERTIES));
      assertThat(activator.started).isFalse();
    }
  }

  @Test
  public void shouldNotStartIfOnlySecondServiceIsRegistered() throws InvalidSyntaxException {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    TestServiceActivator activator = new TestServiceActivator();
    activator.start(bundleContext);
    ServiceEvent service2ServiceEvent = mock(ServiceEvent.class);
    try (MockedStatic<BundleUtils> bundleUtilsMock = mockStatic(BundleUtils.class)) {
      // simulate registration event
      mockService2Notification(bundleContext, service2ServiceEvent, bundleUtilsMock);

      // when
      activator.serviceChanged(service2ServiceEvent);

      // then should not register service
      verify(bundleContext, times(0))
          .registerService(
              eq(TestService.class.getName()), any(TestService.class), eq(EXPECTED_PROPERTIES));
      assertThat(activator.started).isFalse();
    }
  }

  @Test
  public void shouldStartIfBothServicesAreRegistered() throws InvalidSyntaxException {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    TestServiceActivator activator = new TestServiceActivator();
    activator.start(bundleContext);
    ServiceEvent service1ServiceEvent = mock(ServiceEvent.class);
    ServiceEvent service2ServiceEvent = mock(ServiceEvent.class);

    try (MockedStatic<BundleUtils> bundleUtilsMock = mockStatic(BundleUtils.class)) {
      mockService1Notification(bundleContext, service1ServiceEvent, bundleUtilsMock);
      mockService2Notification(bundleContext, service2ServiceEvent, bundleUtilsMock);

      // when
      activator.serviceChanged(service1ServiceEvent);
      activator.serviceChanged(service2ServiceEvent);

      // then should not register service
      verify(bundleContext, times(1))
          .registerService(
              eq(TestService.class.getName()), any(TestService.class), eq(EXPECTED_PROPERTIES));
      assertThat(activator.started).isTrue();
    }
  }

  @Test
  public void shouldStartIfBothServicesAreRegisteredDifferentOrderingOfNotifications()
      throws InvalidSyntaxException {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    TestServiceActivator activator = new TestServiceActivator();
    activator.start(bundleContext);
    ServiceEvent service1ServiceEvent = mock(ServiceEvent.class);
    ServiceEvent service2ServiceEvent = mock(ServiceEvent.class);

    try (MockedStatic<BundleUtils> bundleUtilsMock = mockStatic(BundleUtils.class)) {
      mockService1Notification(bundleContext, service1ServiceEvent, bundleUtilsMock);
      mockService2Notification(bundleContext, service2ServiceEvent, bundleUtilsMock);

      // when
      activator.serviceChanged(service2ServiceEvent);
      activator.serviceChanged(service1ServiceEvent);

      // then should not register service
      verify(bundleContext, times(1))
          .registerService(
              eq(TestService.class.getName()), any(TestService.class), eq(EXPECTED_PROPERTIES));
      assertThat(activator.started).isTrue();
    }
  }

  @Test
  public void shouldStartIfOneServiceIsAccessibleDuringStartAndSecondWasRegisteredLater()
      throws InvalidSyntaxException {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    TestServiceActivator activator = new TestServiceActivator();
    // service 1 is present
    mockService1(bundleContext);
    activator.start(bundleContext);
    ServiceEvent service2ServiceEvent = mock(ServiceEvent.class);

    try (MockedStatic<BundleUtils> bundleUtilsMock = mockStatic(BundleUtils.class)) {
      mockService2Notification(bundleContext, service2ServiceEvent, bundleUtilsMock);

      // when service 2 is registered
      activator.serviceChanged(service2ServiceEvent);

      // then should not register service
      verify(bundleContext, times(1))
          .registerService(
              eq(TestService.class.getName()), any(TestService.class), eq(EXPECTED_PROPERTIES));
      assertThat(activator.started).isTrue();
    }
  }

  @Test
  public void
      shouldStartIfOneServiceIsAccessibleDuringStartAndSecondWasRegisteredLaterDifferentOrdering()
          throws InvalidSyntaxException {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    TestServiceActivator activator = new TestServiceActivator();
    // service 2 is present
    mockService2(bundleContext);
    activator.start(bundleContext);
    ServiceEvent service1ServiceEvent = mock(ServiceEvent.class);

    try (MockedStatic<BundleUtils> bundleUtilsMock = mockStatic(BundleUtils.class)) {
      mockService1Notification(bundleContext, service1ServiceEvent, bundleUtilsMock);

      // when service 1 is registered
      activator.serviceChanged(service1ServiceEvent);

      // then should not register service
      verify(bundleContext, times(1))
          .registerService(
              eq(TestService.class.getName()), any(TestService.class), eq(EXPECTED_PROPERTIES));
      assertThat(activator.started).isTrue();
    }
  }

  @SuppressWarnings("unchecked")
  private void mockService1(BundleContext bundleContext) {
    ServiceReference<DependentService1> serviceReference = mock(ServiceReference.class);
    DependentService1 service = mock(DependentService1.class);
    doReturn(serviceReference)
        .when(bundleContext)
        .getServiceReference(DependentService1.class.getName());
    when(bundleContext.getService(serviceReference)).thenReturn(service);
  }

  @SuppressWarnings("unchecked")
  private void mockService2(BundleContext bundleContext) {
    ServiceReference<DependentService2> serviceReference = mock(ServiceReference.class);
    DependentService2 service = mock(DependentService2.class);
    doReturn(serviceReference)
        .when(bundleContext)
        .getServiceReference(DependentService2.class.getName());
    when(bundleContext.getService(serviceReference)).thenReturn(service);
  }

  private void mockService1Notification(
      BundleContext bundleContext,
      ServiceEvent serviceEvent,
      MockedStatic<BundleUtils> bundleUtilsMock) {
    DependentService1 metrics = mock(DependentService1.class);
    // simulate registration event
    bundleUtilsMock
        .when(
            () ->
                BundleUtils.getRegisteredService(
                    eq(bundleContext), eq(serviceEvent), eq(DependentService1.class)))
        .thenReturn(metrics);
  }

  private void mockService2Notification(
      BundleContext bundleContext,
      ServiceEvent serviceEvent,
      MockedStatic<BundleUtils> bundleUtilsMock) {
    DependentService2 metrics = mock(DependentService2.class);
    // simulate registration event
    bundleUtilsMock
        .when(
            () ->
                BundleUtils.getRegisteredService(
                    eq(bundleContext), eq(serviceEvent), eq(DependentService2.class)))
        .thenReturn(metrics);
  }
}
