package io.stargate.it.cql;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.TestKeyspace;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
public class UdfTest extends BaseIntegrationTest {

  @Test
  @DisplayName("Should create and invoke User-Defined Function")
  public void createAndInvokeUdf(CqlSession session, @TestKeyspace CqlIdentifier keyspace) {
    session.execute(
        "CREATE FUNCTION id(i int) "
            + "RETURNS NULL ON NULL INPUT "
            + "RETURNS int "
            + "LANGUAGE java "
            + "AS 'return i;'");

    Row row =
        session
            .execute(String.format("SELECT %s.id(1) FROM system.local", keyspace.asCql(false)))
            .one();
    assertThat(row.getInt(0)).isEqualTo(1);
  }
}
