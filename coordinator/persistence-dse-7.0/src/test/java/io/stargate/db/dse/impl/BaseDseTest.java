package io.stargate.db.dse.impl;

import io.stargate.db.dse.DsePersistenceActivator;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.virtual.VirtualTables;
import org.junit.jupiter.api.BeforeAll;

public class BaseDseTest {
  /**
   * Only initialize DSE if it isn't already initialized.
   *
   * @throws IOException
   */
  @BeforeAll
  public static void setup() throws IOException {
    if (!DatabaseDescriptor.isDaemonInitialized()) {
      File baseDir = Files.createTempDirectory("stargate-dse-test").toFile();
      baseDir.deleteOnExit();
      DatabaseDescriptor.daemonInitialization(true, DsePersistenceActivator.makeConfig(baseDir));
      VirtualTables.initialize();
    }
  }
}
