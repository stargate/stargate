package io.stargate.db.cassandra.impl;

import static io.stargate.db.cassandra.CassandraPersistenceActivator.makeConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.stargate.db.Persistence;
import io.stargate.it.PersistenceTest;
import io.stargate.it.storage.ClusterConnectionInfo;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import org.apache.cassandra.config.Config;
import org.apache.commons.io.FileUtils;
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
class CassandraPersistenceWithConfigIT extends PersistenceTest {

  private static CassandraPersistence persistence;
  private static File baseDir;
  private static Config config;

  @BeforeAll
  public static void createPersistence(ClusterConnectionInfo backend) throws IOException {
    baseDir = Files.createTempDirectory("stargate-cassandra-4.0-test").toFile();

    System.setProperty("stargate.listen_address", "127.0.0.11");
    System.setProperty("stargate.cluster_name", backend.clusterName());
    System.setProperty("stargate.datacenter", backend.datacenter());
    System.setProperty("stargate.rack", backend.rack());

    // This is the path to the default cassandra.yaml from Cassandra 4.0
    // with row_cache_size_in_mb set to 1024 to test the override.
    System.setProperty(
        "stargate.unsafe.cassandra_config_path", "src/test/resources/cassandra.yaml");
    ClassLoader classLoader = CassandraPersistenceWithConfigIT.class.getClassLoader();
    URL resource = classLoader.getResource("logback-test.xml");

    if (resource != null) {
      File file = new File(resource.getFile());
      System.setProperty("logback.configurationFile", file.getAbsolutePath());
    }

    persistence = new CassandraPersistence();
    config = makeConfig(baseDir);
    persistence.initialize(config);
  }

  @AfterAll
  public static void cleanup() throws IOException {
    // TODO: persistence.destroy() - it fails with a NPE in NativeTransportService.destroy ATM
    //    if (persistence != null) {
    //      persistence.destroy();
    //    }

    FileUtils.deleteDirectory(baseDir);
  }

  @Override
  protected Persistence persistence() {
    return persistence;
  }

  @Test
  public void testOverriddenCassandraConfigField() {
    // Stargate should load the config referenced by `stargate.unsafe.cassandra_config_path`.
    // The value of `row_cache_size_in_mb` in the config is 1024.
    assertEquals(1024, config.row_cache_size_in_mb);
  }
}
