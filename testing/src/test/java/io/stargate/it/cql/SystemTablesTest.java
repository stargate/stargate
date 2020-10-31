package io.stargate.it.cql;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.storage.StargateConnectionInfo;
import io.stargate.it.storage.StargateEnvironmentInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
public class SystemTablesTest extends BaseOsgiIntegrationTest {

  @Test
  @DisplayName("Should expose Stargate address in system.local")
  public void querySystemLocal(CqlSession session, StargateEnvironmentInfo stargate) {
    Row row = session.execute("SELECT * FROM system.local").one();
    assertThat(row).isNotNull();
    assertThat(stargate.nodes())
        .extracting(StargateConnectionInfo::seedAddress)
        .contains(row.getInetAddress("listen_address").getHostAddress());
  }
}
