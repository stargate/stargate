package io.stargate.config.store.k8s.config.map;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.stargate.config.store.ConfigStore;
import okhttp3.TlsVersion;

public class ConfigStoreBuilder {
  private String namespace;
  private String kubernetesMasterUrl;

  public ConfigStoreBuilder withNamespace(String namespace) {
    this.namespace = namespace;
    return this;
  }

  public ConfigStoreBuilder withMasterUrl(String kubernetesMasterUrl) {
    this.kubernetesMasterUrl = kubernetesMasterUrl;
    return this;
  }

  public ConfigStore build() {
    Config config =
        new ConfigBuilder()
            .withMasterUrl(kubernetesMasterUrl)
            .withTrustCerts(true)
            .withTlsVersions(new TlsVersion[] {TlsVersion.TLS_1_0})
            .build();
    DefaultKubernetesClient defaultKubernetesClient = new DefaultKubernetesClient(config);

    return new ConfigStoreK8sConfigMap(namespace, defaultKubernetesClient);
  }
}
