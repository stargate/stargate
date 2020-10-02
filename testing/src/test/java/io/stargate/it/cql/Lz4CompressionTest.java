package io.stargate.it.cql;

import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import io.stargate.it.storage.ClusterConnectionInfo;
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
  public void should_execute_queries_with_lz4_compression() {
    compressionTest();
  }
}
