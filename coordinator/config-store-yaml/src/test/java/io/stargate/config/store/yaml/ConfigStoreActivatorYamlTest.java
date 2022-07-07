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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import io.stargate.config.store.api.ConfigStore;
import io.stargate.core.metrics.api.Metrics;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Hashtable;
import java.util.Objects;
import org.junit.jupiter.api.Test;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Filter;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceReference;

class ConfigStoreActivatorYamlTest {

  @Test
  public void
      shouldRegisterConfigStoreWhenYamlLocationHasExistingStargateConfigAndMetricsServiceIsPresent()
          throws InvalidSyntaxException {

    // given
    BundleContext bundleContext = mock(BundleContext.class);
    mockFilterForMetricsService(bundleContext);
    Path path = getExistingPath();
    ConfigStoreActivator activator = new ConfigStoreActivator(path.toFile().getAbsolutePath());
    Hashtable<String, String> expectedProps = createExpectedProperties();
    activator.start(bundleContext);

    // when
    ServiceReference<Object> serviceReference = mock(ServiceReference.class);
    activator.tracker.startIfAllRegistered(serviceReference, mockMetrics());

    // then
    verify(bundleContext, times(1))
        .registerService(
            eq(ConfigStore.class.getName()), any(ConfigStoreYaml.class), eq(expectedProps));
    assertThat(activator.started).isTrue();
  }

  @Test
  public void
      shouldRegisterConfigStoreWhenYamlLocationHasNotExistingStargateConfigAndMetricsServiceIsPresent()
          throws InvalidSyntaxException {

    // given
    BundleContext bundleContext = mock(BundleContext.class);
    mockFilterForMetricsService(bundleContext);
    ConfigStoreActivator activator = new ConfigStoreActivator("non_existing");
    activator.start(bundleContext);
    Hashtable<String, String> expectedProps = createExpectedProperties();

    // when
    ServiceReference<Object> serviceReference = mock(ServiceReference.class);
    activator.tracker.startIfAllRegistered(serviceReference, mockMetrics());

    // then
    verify(bundleContext, times(1))
        .registerService(
            eq(ConfigStore.class.getName()), any(ConfigStoreYaml.class), eq(expectedProps));
    assertThat(activator.started).isTrue();
  }

  @Test
  public void shouldNotRegisterConfigStoreAndRegisterListenerWhenMetricsServiceIsNotPresent()
      throws InvalidSyntaxException {

    // given
    BundleContext bundleContext = mock(BundleContext.class);
    mockFilterForMetricsService(bundleContext);
    ConfigStoreActivator activator = new ConfigStoreActivator("non_existing");
    activator.start(bundleContext);
    Hashtable<String, String> expectedProps = createExpectedProperties();

    // when non-metrics service registered
    ServiceReference<Object> serviceReference = mock(ServiceReference.class);
    activator.tracker.startIfAllRegistered(serviceReference, mock(Object.class));
    // then
    verify(bundleContext, times(0))
        .registerService(
            eq(ConfigStore.class.getName()), any(ConfigStoreYaml.class), eq(expectedProps));
    assertThat(activator.started).isFalse();
  }

  private Object mockMetrics() {
    Metrics metrics = mock(Metrics.class);
    when(metrics.getRegistry(eq(ConfigStoreActivator.CONFIG_STORE_YAML_METRICS_PREFIX)))
        .thenReturn(new MetricRegistry());
    return metrics;
  }

  private Path getExistingPath() {
    return Paths.get(
        Objects.requireNonNull(
                ConfigStoreActivatorYamlTest.class
                    .getClassLoader()
                    .getResource("stargate-config.yaml"))
            .getPath());
  }

  private void mockFilterForMetricsService(BundleContext bundleContext)
      throws InvalidSyntaxException {
    when(bundleContext.createFilter(String.format("(|(objectClass=%s))", Metrics.class.getName())))
        .thenReturn(mock(Filter.class));
  }

  private Hashtable<String, String> createExpectedProperties() {
    @SuppressWarnings("JdkObsolete")
    Hashtable<String, String> expectedProps = new Hashtable<>();
    expectedProps.put("ConfigStoreIdentifier", ConfigStoreActivator.CONFIG_STORE_YAML_IDENTIFIER);
    return expectedProps;
  }
}
