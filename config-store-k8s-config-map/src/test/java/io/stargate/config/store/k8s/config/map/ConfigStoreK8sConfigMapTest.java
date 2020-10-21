package io.stargate.config.store.k8s.config.map;

import static org.assertj.core.api.Assertions.assertThat;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import io.stargate.config.store.ConfigStore;
import java.net.URISyntaxException;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ConfigStoreK8sConfigMapTest {
  private static KubernetesServer server;

  @BeforeAll
  public static void setup() {
    server = new KubernetesServer(true, true);
    server.before();
  }

  @AfterAll
  public static void cleanup() {
    server.after();
  }

  @Test
  public void shouldGetSettingFromConfigMap() throws URISyntaxException {
    // given
    NamespacedKubernetesClient client = server.getClient();
    String namespace = "ns1";
    // create config maps
    ConfigMap configMap1 =
        new ConfigMapBuilder()
            .withNewMetadata()
            .withName("extension-a")
            .endMetadata()
            .addToData("key", "value")
            .build();
    ConfigMap configMap2 =
        new ConfigMapBuilder()
            .withNewMetadata()
            .withName("extension-b")
            .addToLabels("foo", "bar")
            .endMetadata()
            .addToData("other-setting", "value_2")
            .build();

    client.configMaps().inNamespace(namespace).create(configMap1);
    client.configMaps().inNamespace(namespace).create(configMap2);

    // when
    ConfigStore configStoreK8sConfigMap =
        new ConfigStoreBuilder()
            .withNamespace(namespace)
            .withMasterUrl(client.getMasterUrl().toURI().toString())
            .build();

    // then
    Map<String, String> configForExtension =
        configStoreK8sConfigMap.getConfigForExtension("extension-a");
    assertThat(configForExtension).containsOnly(new SimpleEntry<>("key", "value"));

    Map<String, String> configForExtension2 =
        configStoreK8sConfigMap.getConfigForExtension("extension-b");
    assertThat(configForExtension2).containsOnly(new SimpleEntry<>("other-setting", "value_2"));
  }
}
