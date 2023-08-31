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
package io.stargate.db.cassandra;

import static io.stargate.db.cassandra.Cassandra41PersistenceActivator.makeConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.apache.cassandra.config.Config;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class Cassandra41PersistenceActivatorTest {
  File baseDir;

  @BeforeEach
  void setUp() throws IOException {
    System.clearProperty("stargate.unsafe.cassandra_config_path");
    baseDir = Files.createTempDirectory("stargate-cassandra-4.1-test").toFile();
  }

  @AfterEach
  void tearDown() throws IOException {
    FileUtils.deleteDirectory(baseDir);
  }

  @Test
  void testMakeConfigWithDefaults() throws IOException {
    Config config = makeConfig(baseDir);
    // 0 is the default value of row_cache_size
    assertEquals(0, config.row_cache_size.toMebibytes());
  }

  @Test
  void testMakeConfigWithCustomConfig() throws IOException {
    // This is the path to the default Cassandra 4.1 cassandra.yaml
    // with row_cache_size set to 1024 to test the override.
    System.setProperty(
        "stargate.unsafe.cassandra_config_path", "src/test/resources/cassandra.yaml");
    Config config = makeConfig(baseDir);
    assertEquals(1024, config.row_cache_size.toMebibytes());
  }
}
