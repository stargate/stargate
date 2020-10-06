package io.stargate.it.cql;

import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import io.stargate.it.storage.ClusterConnectionInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class Lz4CompressionTest extends AbstractCompressionTest {

  public Lz4CompressionTest(ClusterConnectionInfo backend) {
    super(backend);
  }

  @Override
  protected void customizeConfig(OptionsMap config) {
    config.put(TypedDriverOption.PROTOCOL_COMPRESSION, "lz4");
  }

  @Test
  @DisplayName("Should connect and execute queries with LZ4 compression")
  public void lz4CompressionTest() {
    compressionTest();
  }
}
