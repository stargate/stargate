package io.stargate.it.cql.protocolV4;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import io.stargate.it.driver.CqlSessionSpec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@CqlSessionSpec(customOptions = "configureLz4")
public class Lz4CompressionTest extends AbstractCompressionTest {

  public static void configureLz4(OptionsMap config) {
    config.put(TypedDriverOption.PROTOCOL_COMPRESSION, "lz4");
    config.put(TypedDriverOption.PROTOCOL_VERSION, "V4");
  }

  @Test
  @DisplayName("Should connect and execute queries with LZ4 compression")
  public void lz4CompressionTest(CqlSession session) {
    compressionTest(session);
  }
}
