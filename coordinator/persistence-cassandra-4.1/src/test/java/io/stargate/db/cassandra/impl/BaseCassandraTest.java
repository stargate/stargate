package io.stargate.db.cassandra.impl;

import io.stargate.db.cassandra.Cassandra41PersistenceActivator;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.junit.jupiter.api.BeforeAll;

public class BaseCassandraTest {
  /**
   * Only initialize Cassandra if it isn't already initialized.
   *
   * @throws IOException
   */
  @BeforeAll
  public static void setup() throws IOException {
    if (!DatabaseDescriptor.isDaemonInitialized()) {
      File baseDir = Files.createTempDirectory("stargate-cassandra-4.1-test").toFile();
      baseDir.deleteOnExit();
      DatabaseDescriptor.daemonInitialization(
          () -> {
            try {
              return Cassandra41PersistenceActivator.makeConfig(baseDir);
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          });
    }
  }
}
