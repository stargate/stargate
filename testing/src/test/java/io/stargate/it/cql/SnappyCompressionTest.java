package io.stargate.it.cql;

import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import io.stargate.it.storage.ClusterConnectionInfo;
import org.junit.jupiter.api.Test;

public class SnappyCompressionTest extends AbstractCompressionTest {

  public SnappyCompressionTest(ClusterConnectionInfo backend) {
    super(backend);
  }

  @Override
  protected void customizeConfig(OptionsMap config) {
    config.put(TypedDriverOption.PROTOCOL_COMPRESSION, "snappy");
  }

  @Test
  public void should_execute_queries_with_snappy_compression() {
    compressionTest();
  }
}
