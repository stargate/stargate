package io.stargate.db.cassandra;

import static io.stargate.db.cassandra.CassandraPersistenceActivator.makeConfig;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.apache.cassandra.config.Config;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CassandraPersistenceActivatorTest {
  File baseDir;

  @BeforeEach
  void setUp() throws IOException {
    System.clearProperty("stargate.unsafe.cassandra_config_path");
    baseDir = Files.createTempDirectory("stargate-cassandra-3.11-test").toFile();
  }

  @AfterEach
  void tearDown() throws IOException {
    FileUtils.deleteDirectory(baseDir);
  }

  @Test
  void testMakeConfigWithDefaults() throws IOException {
    Config config = makeConfig(baseDir);
    // 0 is the default value of row_cache_size_in_mb
    assertEquals(0, config.row_cache_size_in_mb);
  }

  @Test
  void testMakeConfigWithCustomConfig() throws IOException {
    // This is the path to the default Cassandra 3.11.6 cassandra.yaml
    // with row_cache_size_in_mb set to 1024 to test the override.
    System.setProperty(
        "stargate.unsafe.cassandra_config_path", "src/test/resources/cassandra.yaml");
    Config config = makeConfig(baseDir);
    assertEquals(1024, config.row_cache_size_in_mb);
  }
}
