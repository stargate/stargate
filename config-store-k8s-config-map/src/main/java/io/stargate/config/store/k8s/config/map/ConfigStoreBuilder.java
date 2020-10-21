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
package io.stargate.config.store.k8s.config.map;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.stargate.config.store.ConfigStore;

public class ConfigStoreBuilder {
  private String namespace;
  private Config config;

  public ConfigStoreBuilder withNamespace(String namespace) {
    this.namespace = namespace;
    return this;
  }

  public ConfigStoreBuilder withConfig(Config config) {
    this.config = config;
    return this;
  }

  public ConfigStore build() {
    DefaultKubernetesClient defaultKubernetesClient = new DefaultKubernetesClient(config);

    return new ConfigStoreK8sConfigMap(namespace, defaultKubernetesClient);
  }
}
