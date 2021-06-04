package io.stargate.db;

import io.stargate.core.activator.BaseActivator;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public abstract class PersistenceActivator extends BaseActivator {

  private String BASEDIR = System.getProperty("stargate.basedir");

  public PersistenceActivator(String activatorName) {
    super(activatorName);
  }

  /**
   * Creates a throw away data directory for Stargate's ephemeral files. It can be overridden using
   * the system property {@code "stargate.basedir"} which can be useful on systems where the
   * temporary directory is periodically cleaned.
   *
   * @return A file handle to the base directory.
   * @throws IOException if the base directory is invalid or unable to be created.
   */
  protected File getBaseDir() throws IOException {
    if (BASEDIR == null || BASEDIR.isEmpty()) {
      return Files.createTempDirectory("stargate-" + getActivatorName()).toFile();
    } else {
      return Paths.get(BASEDIR).toFile();
    }
  }
}
