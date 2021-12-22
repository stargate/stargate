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

import static io.stargate.core.activator.BaseActivator.ServicePointer.create;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.stargate.core.activator.BaseActivator.LazyServicePointer;
import io.stargate.core.activator.BaseActivator.ServicePointer;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Filter;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;

class BaseActivatorTest {

  @SuppressWarnings("JdkObsolete")
  private static final Hashtable<String, String> EXPECTED_PROPERTIES = new Hashtable<>();

  @SuppressWarnings("JdkObsolete")
  private static final Hashtable<String, String> EMPTY_PROPERTIES = new Hashtable<>();

  static {
    EXPECTED_PROPERTIES.put("Identifier", "id_1");
  }

  @ParameterizedTest
  @MethodSource("dependentServices")
  public void shouldConstructDependenciesFilter(
      List<ServicePointer<?>> serviceDependencies, String expected) {
    // given
    BaseActivator baseActivator = createBaseActivator(serviceDependencies);

    // when
    String dependenciesFilter = baseActivator.constructDependenciesFilter();

    // then
    assertThat(dependenciesFilter).isEqualTo(expected);
  }

  public static Stream<Arguments> dependentServices() {
    return Stream.of(
        arguments(
            Collections.singletonList(create(DependentService1.class)),
            String.format("(|(objectClass=%s))", DependentService1.class.getName())),
        arguments(
            Arrays.asList(create(DependentService1.class), create(DependentService2.class)),
            String.format(
                "(|(objectClass=%s)(objectClass=%s))",
                DependentService1.class.getName(), DependentService2.class.getName())),
        arguments(
            Collections.singletonList(
                ServicePointer.create(DependentService1.class, "Identifier", "Service_1")),
            "(|(Identifier=Service_1))"),
        arguments(
            Arrays.asList(
                ServicePointer.create(DependentService1.class, "Identifier", "Service_1"),
                ServicePointer.create(DependentService2.class, "Identifier2", "Service_2")),
            "(|(Identifier=Service_1)(Identifier2=Service_2))"),
        arguments(
            Arrays.asList(
                create(DependentService1.class),
                ServicePointer.create(DependentService2.class, "Identifier2", "Service_2")),
            String.format(
                "(|(objectClass=%s)(Identifier2=Service_2))", DependentService1.class.getName())));
  }

  @Test
  public void shouldConstructDependenciesFilterWithHeathRegistry() {
    // given
    BaseActivator baseActivator =
        createActivatorWithHeathCheck(Collections.singletonList(create(DependentService1.class)));

    // when
    String dependenciesFilter = baseActivator.constructDependenciesFilter();

    // then
    assertThat(dependenciesFilter)
        .isEqualTo(
            "(|(objectClass=io.stargate.core.activator.DependentService1)(objectClass=com.codahale.metrics.health.HealthCheckRegistry))");
  }

  @ParameterizedTest
  @MethodSource("dependentServicesWithLazy")
  public void shouldConstructDependenciesFilterForNormalAndLazy(
      List<ServicePointer<?>> serviceDependencies,
      List<LazyServicePointer<?>> lazyServiceDependencies,
      String expected) {
    // given
    BaseActivator baseActivator = createBaseActivator(serviceDependencies, lazyServiceDependencies);

    // when
    String dependenciesFilter = baseActivator.constructDependenciesFilter();

    // then
    assertThat(dependenciesFilter).isEqualTo(expected);
  }

  public static Stream<Arguments> dependentServicesWithLazy() {
    return Stream.of(
        arguments(
            Collections.singletonList(create(DependentService1.class)),
            Collections.singletonList(LazyServicePointer.create(DependentService2.class)),
            String.format(
                "(|(objectClass=%s)(objectClass=%s))",
                DependentService1.class.getName(), DependentService2.class.getName())),
        arguments(
            Collections.singletonList(create(DependentService1.class)),
            Collections.singletonList(
                LazyServicePointer.create(DependentService2.class, "Identifier2", "Service_2")),
            String.format(
                "(|(objectClass=%s)(Identifier2=Service_2))", DependentService1.class.getName())),
        arguments(
            Collections.emptyList(),
            Collections.singletonList(LazyServicePointer.create(DependentService2.class)),
            String.format("(|(objectClass=%s))", DependentService2.class.getName())),
        arguments(
            Collections.emptyList(),
            Arrays.asList(
                LazyServicePointer.create(DependentService1.class),
                LazyServicePointer.create(DependentService2.class)),
            String.format(
                "(|(objectClass=%s)(objectClass=%s))",
                DependentService1.class.getName(), DependentService2.class.getName())));
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
  public void shouldStartIfOnlyFirstServiceIsRegisteredInTrackerButSecondOneIsLazy()
      throws InvalidSyntaxException {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    TestServiceActivatorLazy activator = new TestServiceActivatorLazy();
    mockFilterForBothServices(bundleContext);
    activator.start(bundleContext);

    // when
    ServiceReference<Object> serviceReference = mock(ServiceReference.class);
    activator.tracker.startIfAllRegistered(serviceReference, mock(DependentService1.class));

    // then should register service
    verify(bundleContext, times(1))
        .registerService(
            eq(TestServiceLazy.class.getName()),
            any(TestServiceLazy.class),
            eq(EXPECTED_PROPERTIES));
    assertThat(activator.started).isTrue();
  }

  @Test
  public void shouldStartTrackLazyServiceAfterTargetServiceIsStartedAndRegistered()
      throws InvalidSyntaxException {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    TestServiceActivatorLazy activator = new TestServiceActivatorLazy();
    mockFilterForBothServices(bundleContext);
    activator.start(bundleContext);

    // when
    ServiceReference<Object> serviceReference = mock(ServiceReference.class);
    activator.tracker.startIfAllRegistered(serviceReference, mock(DependentService1.class));

    // then should register service
    verify(bundleContext, times(1))
        .registerService(
            eq(TestServiceLazy.class.getName()),
            any(TestServiceLazy.class),
            eq(EXPECTED_PROPERTIES));
    assertThat(activator.started).isTrue();
    assertThat(activator.service2.get().get()).isNull();

    // when lazy service is registered
    activator.tracker.startIfAllRegistered(serviceReference, mock(DependentService2.class));

    // then it should be non-null
    assertThat(activator.service2.get().get()).isNotNull();
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
  public void shouldRegisterTwoServices() throws InvalidSyntaxException {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    TestServiceActivatorTwoServices activator = new TestServiceActivatorTwoServices();
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
    verify(bundleContext, times(1))
        .registerService(
            eq(TestServiceTwo.class.getName()), any(TestServiceTwo.class), eq(EXPECTED_PROPERTIES));
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
  public void shouldStartAndInvokeStopAndUngetIfBothServicesAreRegistered() throws Exception {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    TestServiceActivator activator = new TestServiceActivator();
    mockFilterForBothServices(bundleContext);
    activator.start(bundleContext);
    ServiceReference<?> targetServiceReference = mockServiceRegistration(bundleContext);

    // when
    ServiceReference<Object> serviceReference = mock(ServiceReference.class);
    activator.tracker.startIfAllRegistered(serviceReference, mock(DependentService1.class));
    activator.tracker.startIfAllRegistered(serviceReference, mock(DependentService2.class));

    // then should register service
    verify(bundleContext, times(1))
        .registerService(
            eq(TestService.class.getName()), any(TestService.class), eq(EXPECTED_PROPERTIES));
    assertThat(activator.started).isTrue();

    // when
    activator.stop(bundleContext);

    // then
    assertThat(activator.stopCalled).isTrue();
    // and unget service
    verify(bundleContext, times(1)).ungetService(eq(targetServiceReference));
  }

  @Test
  public void shouldUngetTwoServices() throws Exception {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    TestServiceActivatorTwoServices activator = new TestServiceActivatorTwoServices();
    mockFilterForBothServices(bundleContext);
    activator.start(bundleContext);
    ServiceReference<?> targetServiceReferenceFirst =
        mockServiceRegistration(bundleContext, TestService.class);
    ServiceReference<?> targetServiceReferenceSecond =
        mockServiceRegistration(bundleContext, TestServiceTwo.class);

    // when
    ServiceReference<Object> serviceReference = mock(ServiceReference.class);
    activator.tracker.startIfAllRegistered(serviceReference, mock(DependentService1.class));
    activator.tracker.startIfAllRegistered(serviceReference, mock(DependentService2.class));

    // then should not register service
    verify(bundleContext, times(1))
        .registerService(
            eq(TestService.class.getName()), any(TestService.class), eq(EXPECTED_PROPERTIES));
    verify(bundleContext, times(1))
        .registerService(
            eq(TestServiceTwo.class.getName()), any(TestServiceTwo.class), eq(EXPECTED_PROPERTIES));
    assertThat(activator.started).isTrue();

    // when
    activator.stop(bundleContext);

    // then
    assertThat(activator.stopCalled).isTrue();

    // and unget services
    verify(bundleContext, times(1)).ungetService(eq(targetServiceReferenceFirst));
    verify(bundleContext, times(1)).ungetService(eq(targetServiceReferenceSecond));
  }

  @Test
  public void shouldStartButNotRegisterNotUngetAndInvokeStopIfTargetClassNotSpecified()
      throws Exception {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    TestServiceActivator activator = new TestServiceActivatorWithoutStart();
    mockFilterForBothServices(bundleContext);
    activator.start(bundleContext);
    ServiceReference<?> targetServiceReference = mockServiceRegistration(bundleContext);

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
    // do not unget service
    verify(bundleContext, times(0)).ungetService(eq(targetServiceReference));
  }

  @Test
  public void shouldNotInvokeStopIfWasNotProperlyStarted() throws Exception {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    TestServiceActivator activator = new TestServiceActivatorWithoutStart();
    mockFilterForBothServices(bundleContext);
    activator.start(bundleContext);

    // then should not register service
    verify(bundleContext, times(0))
        .registerService(
            eq(TestService.class.getName()), any(TestService.class), eq(EXPECTED_PROPERTIES));
    assertThat(activator.started).isFalse();

    // when
    activator.stop(bundleContext);

    // then
    assertThat(activator.stopCalled).isFalse();
  }

  @Test
  public void shouldRegisterServiceWithoutDependencies() throws InvalidSyntaxException {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    TestServiceNoDependenciesActivator activator = new TestServiceNoDependenciesActivator();

    // when
    activator.start(bundleContext);

    // then
    verify(bundleContext, times(1))
        .registerService(
            eq(TestServiceNoDependencies.class.getName()),
            any(TestServiceNoDependencies.class),
            eq(EMPTY_PROPERTIES));
    assertThat(activator.started).isTrue();
  }

  @Nested
  public class BaseDir {

    public class TestPersistenceActivator extends BaseActivator {

      public TestPersistenceActivator() {
        super("test-activator");
      }

      @Override
      protected List<ServicePointer<?>> dependencies() {
        return null;
      }
    }

    @Test
    public void happyPathTemp() throws IOException {
      BaseActivator activator = new TestPersistenceActivator();
      File baseDir = activator.getBaseDir();
      assertThat(Files.isDirectory(baseDir.toPath())).isTrue();
      assertThat(Files.isWritable(baseDir.toPath())).isTrue();
    }

    @Test
    public void happyPathSystemProperty() throws IOException {
      try {
        Path temp = Files.createTempDirectory("persist-test");
        Path full = Paths.get(temp.toString(), "name-from-system-property");
        System.setProperty("stargate.basedir", full.toString());
        BaseActivator activator = new TestPersistenceActivator();
        File baseDir = activator.getBaseDir();
        assertThat(Files.isDirectory(baseDir.toPath())).isTrue();
        assertThat(Files.isWritable(baseDir.toPath())).isTrue();
        assertThat(baseDir.toPath())
            .isEqualTo(Paths.get(full.toString(), "stargate-test-activator"));
      } finally {
        System.clearProperty("stargate.basedir");
      }
    }

    @Test
    public void pathAlreadyExists() throws IOException {
      try {
        Path temp = Files.createTempDirectory("persist-test");
        Path full = Paths.get(temp.toString(), "name-from-system-property");

        // Create an existing directory, which is okay
        Files.createDirectory(full);
        assertThat(Files.isDirectory(full)).isTrue();

        System.setProperty("stargate.basedir", full.toString());
        BaseActivator activator = new TestPersistenceActivator();
        File baseDir = activator.getBaseDir();
        assertThat(Files.isDirectory(baseDir.toPath())).isTrue();
        assertThat(Files.isWritable(baseDir.toPath())).isTrue();
        assertThat(baseDir.toPath())
            .isEqualTo(Paths.get(full.toString(), "stargate-test-activator"));
      } finally {
        System.clearProperty("stargate.basedir");
      }
    }

    @Test
    public void pathAlreadyExistsButIsAFile() throws IOException {
      try {
        Path temp = Files.createTempDirectory("persist-test");
        Path full = Paths.get(temp.toString(), "name-from-system-property");

        // Create an existing file, which will cause a failure
        Files.createFile(full);
        assertThat(Files.isRegularFile(full)).isTrue();

        System.setProperty("stargate.basedir", full.toString());
        BaseActivator activator = new TestPersistenceActivator();

        assertThatThrownBy(
                () -> {
                  File baseDir = activator.getBaseDir();
                  assertThat(baseDir).isNotNull(); // Never reached
                })
            .isInstanceOf(IOException.class);
      } finally {
        System.clearProperty("stargate.basedir");
      }
    }
  }

  @SuppressWarnings("unchecked")
  private ServiceReference<?> mockServiceRegistration(BundleContext bundleContext) {
    return mockServiceRegistration(bundleContext, TestService.class);
  }

  private ServiceReference<?> mockServiceRegistration(
      BundleContext bundleContext, Class<?> serviceClass) {
    ServiceRegistration<?> serviceRegistration = mock(ServiceRegistration.class);
    ServiceReference serviceReference = mock(ServiceReference.class);
    when(serviceRegistration.getReference()).thenReturn(serviceReference);
    doReturn(serviceRegistration)
        .when(bundleContext)
        .registerService(eq(serviceClass.getName()), any(serviceClass), eq(EXPECTED_PROPERTIES));
    return serviceReference;
  }

  private BaseActivator createBaseActivator(List<ServicePointer<?>> serviceDependencies) {
    return createBaseActivator(serviceDependencies, Collections.emptyList());
  }

  private BaseActivator createBaseActivator(
      List<ServicePointer<?>> serviceDependencies,
      List<LazyServicePointer<?>> lazyServiceDependencies) {
    return new BaseActivator("ignored") {
      @Override
      protected ServiceAndProperties createService() {
        return null;
      }

      @Override
      protected List<ServicePointer<?>> dependencies() {
        return serviceDependencies;
      }

      @Override
      protected List<LazyServicePointer<?>> lazyDependencies() {
        return lazyServiceDependencies;
      }
    };
  }

  private BaseActivator createActivatorWithHeathCheck(List<ServicePointer<?>> serviceDependencies) {
    return new BaseActivator("test-with-health-check", true) {
      @Override
      protected ServiceAndProperties createService() {
        return null;
      }

      @Override
      protected List<ServicePointer<?>> dependencies() {
        return serviceDependencies;
      }
    };
  }
}
