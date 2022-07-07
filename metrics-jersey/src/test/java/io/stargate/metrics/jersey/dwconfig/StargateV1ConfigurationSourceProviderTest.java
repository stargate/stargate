package io.stargate.metrics.jersey.dwconfig;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

public class StargateV1ConfigurationSourceProviderTest {
  private static final String TEST_MODULE = "testapi";

  // Use base different from standard "config.yaml" to avoid clashes
  private static final String TEST_CONFIG_BASE = "test-config.yaml";

  private static final String TEST_CONFIG_SYSTEM_PROPERTY =
      StargateV1ConfigurationSourceProvider.SYSPROP_CONFIG_FILE_PREFIX + TEST_MODULE;

  // Test for default handling: reading the resource in jar
  @Test
  public void testDefaultConfig() throws Exception {
    // Read expected resource contents directly
    String expectedDefaultConfigs =
        FileUtils.readFileToString(
                new File("src/test/resources/" + TEST_CONFIG_BASE), StandardCharsets.UTF_8)
            .trim();
    String actualDefaultConfigs = readConfig();
    assertThat(actualDefaultConfigs).isEqualTo(expectedDefaultConfigs);
  }

  // Test to verify that setting specific System property will use override too
  @Test
  public void testViaSystemProperty() throws Exception {
    final String ALT_CONFIG_PATH = "src/test/resources/alt-" + TEST_CONFIG_BASE;
    System.setProperty(TEST_CONFIG_SYSTEM_PROPERTY, ALT_CONFIG_PATH);
    try {
      assertThat(readConfig()).isEqualTo("config: alt");
    } finally {
      System.clearProperty(TEST_CONFIG_SYSTEM_PROPERTY);
    }
  }

  // Test to verify that inclusion of an alternate configuration based on naming convention
  @Test
  public void testNamingConventionBased() throws Exception {
    // Bit unclean as we need be able to co-locate alternative config file to our CWD,
    // ensuring it is not seen during other tests
    final String originalCWD = System.getProperty("user.dir");
    try {
      File f = new File(originalCWD);
      f = new File(f, "src");
      f = new File(f, "test");
      f = new File(f, "resources");
      f = new File(f, "cwd");
      System.setProperty("user.dir", f.toString());
      assertThat(readConfig()).isEqualTo("config: bogus");
    } finally {
      System.setProperty("user.dir", originalCWD);
    }
  }

  private String readConfig() throws Exception {
    try (InputStream in =
        new StargateV1ConfigurationSourceProvider(TEST_MODULE).open(TEST_CONFIG_BASE)) {
      return IOUtils.toString(in, StandardCharsets.UTF_8).trim();
    }
  }
}
