package io.stargate.it.cql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import io.stargate.it.storage.ClusterConnectionInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class UseKeyspaceTest extends JavaDriverTestBase {

  public UseKeyspaceTest(ClusterConnectionInfo backend) {
    super(backend);
  }

  @BeforeEach
  public void setupSchema() {
    session.execute("CREATE TABLE IF NOT EXISTS test (k INT PRIMARY KEY)");
  }

  @Test
  @DisplayName("Should fail unqualified query if not logged into any keyspace")
  public void useKeyspace() {
    CqlSession unloggedSession = newSessionBuilder().build();
    assertThatThrownBy(() -> unloggedSession.execute("SELECT * FROM TEST WHERE k=1"))
        .isInstanceOf(InvalidQueryException.class)
        .hasMessage(
            "No keyspace has been specified. USE a keyspace, or explicitly specify keyspace.tablename");

    // Switch to keyspace and retry query
    unloggedSession.execute(String.format("USE %s", keyspaceId.asCql(false)));
    Row row = unloggedSession.execute("SELECT * FROM TEST WHERE k=1").one();
    assertThat(row).isNull();
  }
}
