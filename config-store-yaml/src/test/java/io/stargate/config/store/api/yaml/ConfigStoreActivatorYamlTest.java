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
package io.stargate.config.store.api.yaml;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.stargate.config.store.api.ConfigStore;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Hashtable;
import java.util.Objects;
import org.junit.jupiter.api.Test;
import org.osgi.framework.BundleContext;

class ConfigStoreActivatorYamlTest {

  @Test
  public void shouldRegisterConfigStoreWhenYamlLocationHasExistingStargateConfig() {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    Path path =
        Paths.get(
            Objects.requireNonNull(
                    ConfigStoreActivatorYamlTest.class
                        .getClassLoader()
                        .getResource("stargate-config.yaml"))
                .getPath());
    ConfigStoreActivator activator = new ConfigStoreActivator(path.toFile().getAbsolutePath());

    // when
    activator.start(bundleContext);

    Hashtable<String, String> expectedProps = new Hashtable<>();
    expectedProps.put("Identifier", ConfigStoreActivator.CONFIG_STORE_YAML_IDENTIFIER);
    // then
    verify(bundleContext, times(1))
        .registerService(eq(ConfigStore.class), any(ConfigStoreYaml.class), eq(expectedProps));
  }

  @Test
  public void shouldRegisterConfigStoreWhenYamlLocationHasNotExistingStargateConfig() {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    ConfigStoreActivator activator = new ConfigStoreActivator("non_existing");

    // when
    activator.start(bundleContext);

    Hashtable<String, String> expectedProps = new Hashtable<>();
    expectedProps.put("Identifier", ConfigStoreActivator.CONFIG_STORE_YAML_IDENTIFIER);
    // then
    verify(bundleContext, times(1))
        .registerService(eq(ConfigStore.class), any(ConfigStoreYaml.class), eq(expectedProps));
  }
}
