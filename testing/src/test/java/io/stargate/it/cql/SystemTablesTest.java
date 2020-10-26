package io.stargate.it.cql;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.cql.Row;
import java.net.UnknownHostException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class SystemTablesTest extends JavaDriverTestBase {

  @Test
  @DisplayName("Should expose Stargate address in system.local")
  public void querySystemLocal() throws UnknownHostException {
    Row row = session.execute("SELECT * FROM system.local").one();
    assertThat(row).isNotNull();
    assertThat(row.getInetAddress("listen_address")).isIn(getStargateInetSocketAddresses());
  }
}
