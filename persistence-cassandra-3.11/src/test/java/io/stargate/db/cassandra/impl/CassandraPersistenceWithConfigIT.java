package io.stargate.db.cassandra.impl;

import static io.stargate.db.cassandra.impl.CassandraPersistenceIT.initializePersistence;
import static io.stargate.db.cassandra.impl.CassandraPersistenceIT.setUpConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.stargate.it.storage.ClusterConnectionInfo;
import io.stargate.it.storage.ClusterSpec;
import io.stargate.it.storage.ExternalStorage;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.apache.cassandra.config.Config;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

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
@ExtendWith(ExternalStorage.class)
class CassandraPersistenceWithConfigIT {
  // This is the path to the default cassandra.yaml from Cassandra 3.11.6
  // with row_cache_size_in_mb set to 1024 to test the override.
  private static final String CASSANDRA_CONFIG_PATH = "src/test/resources/cassandra.yaml";
  private static final String CASSANDRA_CONFIG_PATH_PROPERTY =
      "stargate.unsafe.cassandra_config_path";
  private static File baseDir;
  private static Config config;

  public void setUp(ClusterConnectionInfo backend) throws IOException {
    baseDir = Files.createTempDirectory("stargate-cassandra-3.11-test").toFile();
    config = setUpConfig(backend, baseDir);
    initializePersistence(config);
  }

  @AfterEach
  public void cleanUp() throws IOException {
    FileUtils.deleteDirectory(baseDir);
  }

  @Test
  @ClusterSpec
  public void testDefaultCassandraConfigField(ClusterConnectionInfo backend) throws IOException {
    setUp(backend);
    assertEquals(0, config.row_cache_size_in_mb);
  }

  @Test
  @ClusterSpec
  public void testOverriddenCassandraConfigField(ClusterConnectionInfo backend) throws IOException {
    // Stargate should load the config referenced by `stargate.unsafe.cassandra_config_path`.
    // The value of `row_cache_size_in_mb` in the config is 1024.
    System.setProperty(CASSANDRA_CONFIG_PATH_PROPERTY, CASSANDRA_CONFIG_PATH);
    setUp(backend);
    System.clearProperty(CASSANDRA_CONFIG_PATH_PROPERTY);
    assertEquals(1024, config.row_cache_size_in_mb);
  }
}
