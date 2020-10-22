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
import java.util.Objects;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.osgi.framework.BundleContext;

class ConfigStoreActivatorYamlExistsTest {

  @BeforeAll
  public static void setup() {
    Path path =
        Paths.get(
            Objects.requireNonNull(
                    ConfigStoreActivatorYamlExistsTest.class
                        .getClassLoader()
                        .getResource("stargate-config.yaml"))
                .getPath());
    System.setProperty("stargate.config_store.yaml.location", path.toFile().getAbsolutePath());
  }

  @Test
  public void shouldRegisterConfigStoreWhenYamlLocationHasExistingStargateConfig() {
    // given
    BundleContext bundleContext = mock(BundleContext.class);
    ConfigStoreActivator kafkaProducerActivator = new ConfigStoreActivator();

    // when
    kafkaProducerActivator.start(bundleContext);

    // then
    verify(bundleContext, times(1))
        .registerService(eq(ConfigStore.class), any(ConfigStoreYaml.class), any());
  }
}
