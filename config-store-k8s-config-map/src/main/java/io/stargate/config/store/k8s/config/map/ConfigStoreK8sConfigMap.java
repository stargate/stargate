package io.stargate.config.store.k8s.config.map;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.DoneableConfigMap;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.stargate.config.store.ConfigStore;
import java.io.IOException;
import java.util.Map;

public class ConfigStoreK8sConfigMap implements ConfigStore {
  private final NamespacedKubernetesClient client;
  private final String namespace;

  public ConfigStoreK8sConfigMap(String namespace, NamespacedKubernetesClient client) {
    this.namespace = namespace;
    this.client = client;
  }

  @Override
  public Map<String, String> getConfigForExtension(String extensionName) {
    Resource<ConfigMap, DoneableConfigMap> configMaps =
        client.configMaps().inNamespace(namespace).withName(extensionName);
    return configMaps.get().getData();
  }

  @Override
  public void close() throws IOException {
    client.close();
  }
}
