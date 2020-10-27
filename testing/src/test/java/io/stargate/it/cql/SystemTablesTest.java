package io.stargate.it.cql;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import java.net.UnknownHostException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
public class SystemTablesTest extends BaseOsgiIntegrationTest {

  @Test
  @DisplayName("Should expose Stargate address in system.local")
  public void querySystemLocal(CqlSession session) throws UnknownHostException {
    Row row = session.execute("SELECT * FROM system.local").one();
    assertThat(row).isNotNull();
    assertThat(row.getInetAddress("listen_address")).isIn(STARGATE_ADDRESSES);
  }
}
