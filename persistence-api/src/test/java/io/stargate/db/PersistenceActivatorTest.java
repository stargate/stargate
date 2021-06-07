package io.stargate.db;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class PersistenceActivatorTest {

  @Nested
  public class BaseDir {

    public class TestPersistenceActivator extends PersistenceActivator {

      public TestPersistenceActivator() {
        super("test");
      }

      @Override
      protected List<ServicePointer<?>> dependencies() {
        return null;
      }
    }

    @Test
    public void happyPathTemp() throws IOException {
      PersistenceActivator activator = new TestPersistenceActivator();
      File baseDir = activator.getBaseDir();
      assertThat(Files.isDirectory(baseDir.toPath())).isTrue();
      assertThat(Files.isWritable(baseDir.toPath())).isTrue();
    }

    @Test
    public void happyPathSystemProperty() throws IOException {
      try {
        Path temp = Files.createTempDirectory("persist-test");
        Path full = Paths.get(temp.toString(), "name-from-system-property");
        System.setProperty("stargate.basedir", full.toString());
        PersistenceActivator activator = new TestPersistenceActivator();
        File baseDir = activator.getBaseDir();
        assertThat(Files.isDirectory(baseDir.toPath())).isTrue();
        assertThat(Files.isWritable(baseDir.toPath())).isTrue();
        assertThat(baseDir.toPath()).isEqualTo(full);
      } finally {
        System.clearProperty("stargate.basedir");
      }
    }

    @Test
    public void pathAlreadyExists() throws IOException {
      try {
        Path temp = Files.createTempDirectory("persist-test");
        Path full = Paths.get(temp.toString(), "name-from-system-property");

        // Create an existing directory, which is okay
        Files.createDirectory(full);
        assertThat(Files.isDirectory(full)).isTrue();

        System.setProperty("stargate.basedir", full.toString());
        PersistenceActivator activator = new TestPersistenceActivator();
        File baseDir = activator.getBaseDir();
        assertThat(Files.isDirectory(baseDir.toPath())).isTrue();
        assertThat(Files.isWritable(baseDir.toPath())).isTrue();
        assertThat(baseDir.toPath()).isEqualTo(full);
      } finally {
        System.clearProperty("stargate.basedir");
      }
    }

    @Test
    public void pathAlreadyExistsButIsAFile() throws IOException {
      try {
        Path temp = Files.createTempDirectory("persist-test");
        Path full = Paths.get(temp.toString(), "name-from-system-property");

        // Create an existing file, which will cause a failure
        Files.createFile(full);
        assertThat(Files.isRegularFile(full)).isTrue();

        System.setProperty("stargate.basedir", full.toString());
        PersistenceActivator activator = new TestPersistenceActivator();

        assertThatThrownBy(
                () -> {
                  File baseDir = activator.getBaseDir();
                  assertThat(baseDir).isNotNull(); // Never reached
                })
            .isInstanceOf(FileAlreadyExistsException.class);
      } finally {
        System.clearProperty("stargate.basedir");
      }
    }
  }
}
