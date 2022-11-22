package io.stargate.db.dse.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.bdp.config.DseConfig;
import com.datastax.oss.driver.shaded.guava.common.io.Resources;
import io.stargate.db.Persistence;
import io.stargate.db.dse.DsePersistenceActivator;
import io.stargate.it.PersistenceTest;
import io.stargate.it.storage.ClusterConnectionInfo;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.util.Objects;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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
public class DsePersistenceIT extends PersistenceTest {

  private static DsePersistence persistence;

  @BeforeAll
  public static void createDsePersistence(ClusterConnectionInfo backend) throws IOException {
    File baseDir = Files.createTempDirectory("stargate-dse-test").toFile();
    baseDir.deleteOnExit();

    System.setProperty("stargate.listen_address", "127.0.0.11");
    System.setProperty("stargate.cluster_name", backend.clusterName());
    System.setProperty("stargate.datacenter", backend.datacenter());
    System.setProperty("stargate.rack", backend.rack());
    ClassLoader classLoader = DsePersistenceIT.class.getClassLoader();
    URL resource = classLoader.getResource("logback-test.xml");

    if (resource != null) {
      File file = new File(resource.getFile());
      System.setProperty("logback.configurationFile", file.getAbsolutePath());
    }

    // Setup a custom `dse.yaml`
    File f = null;
    try (OutputStream os =
        Files.newOutputStream((f = File.createTempFile("dse", ".yaml", null)).toPath())) {
      Resources.copy(
          Objects.requireNonNull(
              DsePersistenceIT.class.getResource("/dse-test.yaml"),
              "Unable to load '/dse-test.yaml' resource"),
          os);
    }
    System.setProperty(DsePersistence.SYSPROP_UNSAFE_DSE_CONFIG_PATH, f.getAbsolutePath());

    persistence = new DsePersistence();
    persistence.initialize(DsePersistenceActivator.makeConfig(baseDir));
  }

  @AfterAll
  public static void cleanup() throws IOException {
    if (persistence != null) {
      persistence.destroy();
    }
  }

  @Test
  public void verifyDseYamlFileSettings() throws IOException {
    // Verify settings loaded from custom `dse.yaml`
    assertThat(DseConfig.getauditLoggingOptions().retention_time).isEqualTo(12345);
    assertThat(DseConfig.getInternodeMessagingPort()).isEqualTo(54321);
  }

  @Override
  protected Persistence persistence() {
    return persistence;
  }
}
