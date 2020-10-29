package io.stargate.it.cql;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import io.stargate.it.driver.CqlSessionSpec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@CqlSessionSpec(customOptions = "configureSnappy")
public class SnappyCompressionTest extends AbstractCompressionTest {

  public static void configureSnappy(OptionsMap config) {
    config.put(TypedDriverOption.PROTOCOL_COMPRESSION, "snappy");
  }

  @Test
  @DisplayName("Should connect and execute queries with Snappy compression")
  public void snappyCompressionTest(CqlSession session) {
    compressionTest(session);
  }
}
