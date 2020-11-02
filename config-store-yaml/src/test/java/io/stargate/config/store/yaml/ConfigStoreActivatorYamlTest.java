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
package io.stargate.config.store.yaml;

import static org.assertj.core.api.Assertions.assertThat;
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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Hashtable;
import java.util.Objects;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.osgi.framework.BundleContext;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceEvent;
import org.osgi.framework.ServiceReference;

class ConfigStoreActivatorYamlTest {

  @Test
  public void
      shouldRegisterConfigStoreWhenYamlLocationHasExistingStargateConfigAndMetricsServiceIsPresent()
          throws InvalidSyntaxException {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    Path path = getExistingPath();
    ConfigStoreActivator activator = new ConfigStoreActivator(path.toFile().getAbsolutePath());
    mockMetrics(bundleContext);

    // when
    activator.start(bundleContext);

    Hashtable<String, String> expectedProps = createExpectedProperties();
    // then
    verify(bundleContext, times(1))
        .registerService(eq(ConfigStore.class), any(ConfigStoreYaml.class), eq(expectedProps));
    assertThat(activator.started).isTrue();
  }

  @Test
  public void
      shouldRegisterConfigStoreWhenYamlLocationHasNotExistingStargateConfigAndMetricsServiceIsPresent()
          throws InvalidSyntaxException {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    ConfigStoreActivator activator = new ConfigStoreActivator("non_existing");
    mockMetrics(bundleContext);

    // when
    activator.start(bundleContext);

    Hashtable<String, String> expectedProps = createExpectedProperties();
    // then
    verify(bundleContext, times(1))
        .registerService(eq(ConfigStore.class), any(ConfigStoreYaml.class), eq(expectedProps));
    assertThat(activator.started).isTrue();
  }

  @Test
  public void shouldNotRegisterConfigStoreAndRegisterListenerWhenMetricsServiceIsNotPresent()
      throws InvalidSyntaxException {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    Path path = getExistingPath();
    ConfigStoreActivator activator = new ConfigStoreActivator(path.toFile().getAbsolutePath());

    // when
    activator.start(bundleContext);

    Hashtable<String, String> expectedProps = createExpectedProperties();
    // then
    verify(bundleContext, times(1))
        .addServiceListener(any(), eq(String.format("(objectClass=%s)", Metrics.class.getName())));
    verify(bundleContext, times(0))
        .registerService(eq(ConfigStore.class), any(ConfigStoreYaml.class), eq(expectedProps));
    assertThat(activator.started).isFalse();
  }

  @Test
  public void shouldStartWhenMetricsServiceChangedNotification() throws InvalidSyntaxException {
    // given
    Path path = getExistingPath();
    BundleContext bundleContext = mock(BundleContext.class);
    ConfigStoreActivator activator = new ConfigStoreActivator(path.toFile().getAbsolutePath());
    activator.start(bundleContext);
    ServiceEvent metricsServiceEvent = mock(ServiceEvent.class);

    try (MockedStatic<BundleUtils> bundleUtilsMock = mockStatic(BundleUtils.class)) {
      mockMetricsServiceNotification(bundleContext, metricsServiceEvent, bundleUtilsMock);
      Hashtable<String, String> expectedProps = createExpectedProperties();

      // when
      activator.serviceChanged(metricsServiceEvent);

      // then should register service
      verify(bundleContext, times(1))
          .registerService(eq(ConfigStore.class), any(ConfigStoreYaml.class), eq(expectedProps));
      assertThat(activator.started).isTrue();
    }
  }

  @Test
  public void shouldNotStartWhenMetricsServiceChangedNotificationReturnsNull()
      throws InvalidSyntaxException {
    // given
    Path path = getExistingPath();
    BundleContext bundleContext = mock(BundleContext.class);
    ConfigStoreActivator activator = new ConfigStoreActivator(path.toFile().getAbsolutePath());
    activator.start(bundleContext);
    ServiceEvent metricsServiceEvent = mock(ServiceEvent.class);

    Hashtable<String, String> expectedProps = createExpectedProperties();

    // when
    activator.serviceChanged(metricsServiceEvent);

    // then should register service
    verify(bundleContext, times(0))
        .registerService(eq(ConfigStore.class), any(ConfigStoreYaml.class), eq(expectedProps));
    assertThat(activator.started).isFalse();
  }

  @SuppressWarnings("unchecked")
  private void mockMetrics(BundleContext bundleContext) {
    ServiceReference<Metrics> metricsServiceReference = mock(ServiceReference.class);
    Metrics metrics = mock(Metrics.class);
    doReturn(metricsServiceReference)
        .when(bundleContext)
        .getServiceReference(Metrics.class.getName());
    when(bundleContext.getService(metricsServiceReference)).thenReturn(metrics);
    when(metrics.getRegistry(eq(ConfigStoreActivator.CONFIG_STORE_YAML_METRICS_PREFIX)))
        .thenReturn(new MetricRegistry());
  }

  private Path getExistingPath() {
    return Paths.get(
        Objects.requireNonNull(
                ConfigStoreActivatorYamlTest.class
                    .getClassLoader()
                    .getResource("stargate-config.yaml"))
            .getPath());
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

  private Hashtable<String, String> createExpectedProperties() {
    Hashtable<String, String> expectedProps = new Hashtable<>();
    expectedProps.put("Identifier", ConfigStoreActivator.CONFIG_STORE_YAML_IDENTIFIER);
    return expectedProps;
  }
}
