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

import static io.stargate.core.activator.BaseActivator.DependentService.constructDependentService;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.stargate.core.activator.BaseActivator.DependentService;
import java.util.Arrays;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Filter;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceReference;

class BaseActivatorTest {

  private static Hashtable<String, String> EXPECTED_PROPERTIES = new Hashtable<>();

  static {
    EXPECTED_PROPERTIES.put("Identifier", "id_1");
  }

  @ParameterizedTest
  @MethodSource("dependentServices")
  public void shouldConstructDependenciesFilter(
      List<DependentService> dependentServices, String expected) {
    // given
    BaseActivator baseActivator = createBaseActivator(dependentServices);

    // when
    String dependenciesFilter = baseActivator.constructDependenciesFilter();

    // then
    assertThat(dependenciesFilter).isEqualTo(expected);
  }

  public static Stream<Arguments> dependentServices() {
    return Stream.of(
        arguments(
            Collections.singletonList(constructDependentService(DependentService1.class)),
            String.format("(|(objectClass=%s))", DependentService1.class.getName())),
        arguments(
            Arrays.asList(
                constructDependentService(DependentService1.class),
                constructDependentService(DependentService2.class)),
            String.format(
                "(|(objectClass=%s)(objectClass=%s))",
                DependentService1.class.getName(), DependentService2.class.getName())),
        arguments(
            Collections.singletonList(
                constructDependentService(DependentService1.class, "Identifier", "Service_1")),
            "(|(Identifier=Service_1))"),
        arguments(
            Arrays.asList(
                constructDependentService(DependentService1.class, "Identifier", "Service_1"),
                constructDependentService(DependentService2.class, "Identifier2", "Service_2")),
            "(|(Identifier=Service_1)(Identifier2=Service_2))"),
        arguments(
            Arrays.asList(
                constructDependentService(DependentService1.class),
                constructDependentService(DependentService2.class, "Identifier2", "Service_2")),
            String.format(
                "(|(objectClass=%s)(Identifier2=Service_2))", DependentService1.class.getName())));
  }

  @Test
  public void shouldNotStartService() throws InvalidSyntaxException {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    TestServiceActivator activator = new TestServiceActivator();
    mockFilterForBothServices(bundleContext);

    // when
    activator.start(bundleContext);

    // then service not started
    assertThat(activator.started).isFalse();
  }

  private void mockFilterForBothServices(BundleContext bundleContext)
      throws InvalidSyntaxException {
    when(bundleContext.createFilter(
            String.format(
                "(|(objectClass=%s)(objectClass=%s))",
                DependentService1.class.getName(), DependentService2.class.getName())))
        .thenReturn(mock(Filter.class));
  }

  @Test
  public void shouldNotStartIfOnlyFirstServiceIsRegisteredInTracker()
      throws InvalidSyntaxException {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    TestServiceActivator activator = new TestServiceActivator();
    mockFilterForBothServices(bundleContext);
    activator.start(bundleContext);

    // when
    ServiceReference<Object> serviceReference = mock(ServiceReference.class);
    activator.tracker.startIfAllRegistered(serviceReference, mock(DependentService1.class));

    // then should not register service
    verify(bundleContext, times(0))
        .registerService(
            eq(TestService.class.getName()), any(TestService.class), eq(EXPECTED_PROPERTIES));
    assertThat(activator.started).isFalse();
  }

  @Test
  public void shouldNotStartIfOnlySecondServiceIsRegisteredInTracker()
      throws InvalidSyntaxException {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    TestServiceActivator activator = new TestServiceActivator();
    mockFilterForBothServices(bundleContext);
    activator.start(bundleContext);

    // when
    ServiceReference<Object> serviceReference = mock(ServiceReference.class);
    activator.tracker.startIfAllRegistered(serviceReference, mock(DependentService2.class));

    // then should not register service
    verify(bundleContext, times(0))
        .registerService(
            eq(TestService.class.getName()), any(TestService.class), eq(EXPECTED_PROPERTIES));
    assertThat(activator.started).isFalse();
  }

  @Test
  public void shouldNotStartIfRegisterNull() throws InvalidSyntaxException {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    TestServiceActivator activator = new TestServiceActivator();
    mockFilterForBothServices(bundleContext);
    activator.start(bundleContext);

    // when
    ServiceReference<Object> serviceReference = mock(ServiceReference.class);
    activator.tracker.startIfAllRegistered(serviceReference, null);

    // then should not register service
    verify(bundleContext, times(0))
        .registerService(
            eq(TestService.class.getName()), any(TestService.class), eq(EXPECTED_PROPERTIES));
    assertThat(activator.started).isFalse();
  }

  @Test
  public void shouldStartIfBothServicesAreRegistered() throws InvalidSyntaxException {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    TestServiceActivator activator = new TestServiceActivator();
    mockFilterForBothServices(bundleContext);
    activator.start(bundleContext);

    // when
    ServiceReference<Object> serviceReference = mock(ServiceReference.class);
    activator.tracker.startIfAllRegistered(serviceReference, mock(DependentService1.class));
    activator.tracker.startIfAllRegistered(serviceReference, mock(DependentService2.class));

    // then should not register service
    verify(bundleContext, times(1))
        .registerService(
            eq(TestService.class.getName()), any(TestService.class), eq(EXPECTED_PROPERTIES));
    assertThat(activator.started).isTrue();
  }

  @Test
  public void shouldStartIfBothServicesAreDifferentOrderingOfNotifications()
      throws InvalidSyntaxException {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    TestServiceActivator activator = new TestServiceActivator();
    mockFilterForBothServices(bundleContext);
    activator.start(bundleContext);

    // when
    ServiceReference<Object> serviceReference = mock(ServiceReference.class);
    activator.tracker.startIfAllRegistered(serviceReference, mock(DependentService2.class));
    activator.tracker.startIfAllRegistered(serviceReference, mock(DependentService1.class));

    // then should not register service
    verify(bundleContext, times(1))
        .registerService(
            eq(TestService.class.getName()), any(TestService.class), eq(EXPECTED_PROPERTIES));
    assertThat(activator.started).isTrue();
  }

  @Test
  public void shouldRegisterServiceOnlyOnceEvenIfStartIsCalledSecondTime()
      throws InvalidSyntaxException {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    TestServiceActivator activator = new TestServiceActivator();
    mockFilterForBothServices(bundleContext);
    activator.start(bundleContext);

    // when both services provided
    ServiceReference<Object> serviceReference = mock(ServiceReference.class);
    activator.tracker.startIfAllRegistered(serviceReference, mock(DependentService2.class));
    activator.tracker.startIfAllRegistered(serviceReference, mock(DependentService1.class));

    // then register service and start
    verify(bundleContext, times(1))
        .registerService(
            eq(TestService.class.getName()), any(TestService.class), eq(EXPECTED_PROPERTIES));
    assertThat(activator.started).isTrue();

    // when start 2nd time
    Mockito.reset(bundleContext); // reset the invocations counter
    activator.tracker.startIfAllRegistered(serviceReference, mock(DependentService2.class));
    activator.tracker.startIfAllRegistered(serviceReference, mock(DependentService1.class));

    // then should not register service and remain started
    verify(bundleContext, times(0))
        .registerService(
            eq(TestService.class.getName()), any(TestService.class), eq(EXPECTED_PROPERTIES));
    assertThat(activator.started).isTrue();
  }

  @Test
  public void shouldStartAndInvokeStopIfBothServicesAreRegistered() throws Exception {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    TestServiceActivator activator = new TestServiceActivator();
    mockFilterForBothServices(bundleContext);
    activator.start(bundleContext);

    // when
    ServiceReference<Object> serviceReference = mock(ServiceReference.class);
    activator.tracker.startIfAllRegistered(serviceReference, mock(DependentService1.class));
    activator.tracker.startIfAllRegistered(serviceReference, mock(DependentService2.class));

    // then should not register service
    verify(bundleContext, times(1))
        .registerService(
            eq(TestService.class.getName()), any(TestService.class), eq(EXPECTED_PROPERTIES));
    assertThat(activator.started).isTrue();

    // when
    activator.stop(bundleContext);

    // then
    assertThat(activator.stopCalled).isTrue();
  }

  @Test
  public void shouldStartButNotRegisterAndInvokeStopIfTargetClassNotSpecified() throws Exception {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    TestServiceActivator activator = new TestServiceActivator(null);
    mockFilterForBothServices(bundleContext);
    activator.start(bundleContext);

    // when
    ServiceReference<Object> serviceReference = mock(ServiceReference.class);
    activator.tracker.startIfAllRegistered(serviceReference, mock(DependentService1.class));
    activator.tracker.startIfAllRegistered(serviceReference, mock(DependentService2.class));

    // then should not register service
    verify(bundleContext, times(0))
        .registerService(
            eq(TestService.class.getName()), any(TestService.class), eq(EXPECTED_PROPERTIES));
    assertThat(activator.started).isTrue();

    // when
    activator.stop(bundleContext);

    // then
    assertThat(activator.stopCalled).isTrue();
  }

  private BaseActivator createBaseActivator(List<DependentService> dependentServices) {

    return new BaseActivator("ignored", dependentServices, Object.class) {
      @Override
      protected ServiceAndProperties createService(List<Object> dependentServices1) {
        return null;
      }

      @Override
      protected void stopService() {
        // no-op
      }
    };
  }
}
