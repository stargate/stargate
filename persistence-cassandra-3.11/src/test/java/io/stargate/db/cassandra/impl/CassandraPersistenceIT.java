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
import java.util.Optional;
import org.apache.cassandra.config.Config;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

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
class CassandraPersistenceIT extends PersistenceTest {

  private static CassandraPersistence persistence;
  private static File baseDir;
  private static Config config;

  @BeforeEach
  public void createPersistence(TestInfo testInfo, ClusterConnectionInfo backend)
      throws IOException {
    baseDir = Files.createTempDirectory("stargate-cassandra-3.11-test").toFile();

    System.setProperty("stargate.listen_address", "127.0.0.11");
    System.setProperty("stargate.cluster_name", backend.clusterName());
    System.setProperty("stargate.datacenter", backend.datacenter());
    System.setProperty("stargate.rack", backend.rack());

    // This is the path to the default cassandra.yaml from Cassandra 3.11.6
    // with row_cache_size_in_mb set to 1024 to test the override.
    getCassandraConfigPath()
        .ifPresent(
            configPath -> System.setProperty("stargate.unsafe.cassandra_config_path", configPath));

    ClassLoader classLoader = CassandraPersistenceIT.class.getClassLoader();
    URL resource = classLoader.getResource("logback-test.xml");

    if (resource != null) {
      File file = new File(resource.getFile());
      System.setProperty("logback.configurationFile", file.getAbsolutePath());
    }

    persistence = new CassandraPersistence();
    config = makeConfig(baseDir);
    persistence.initialize(config);
    setup(testInfo, backend);
  }

  @AfterEach
  public void cleanup() throws IOException {
    // TODO: persistence.destroy() - note: it gets an NPE in NativeTransportService.destroy ATM
    //    if (persistence != null) {
    //      persistence.destroy();
    //    }

    FileUtils.deleteDirectory(baseDir);
  }

  @Override
  protected Persistence persistence() {
    return persistence;
  }

  protected Optional<String> getCassandraConfigPath() {
    return Optional.empty();
  }

  protected long getExpectedRowCacheSizeInMb() {
    return 0;
  }

  @Test
  public void testOverriddenCassandraConfigField() {
    assertEquals(getExpectedRowCacheSizeInMb(), config.row_cache_size_in_mb);
  }
}
