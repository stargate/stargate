package io.stargate.it.cql.compatibility.protocolv4;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    initQueries = "CREATE TABLE IF NOT EXISTS test (k INT PRIMARY KEY)",
    customOptions = "applyProtocolVersion")
public class UseKeyspaceTest extends BaseIntegrationTest {

  public static void applyProtocolVersion(OptionsMap config) {
    config.put(TypedDriverOption.PROTOCOL_VERSION, "V4");
  }

  @Test
  @DisplayName("Should fail unqualified query if not logged into any keyspace")
  public void useKeyspace(CqlSessionBuilder builder, @TestKeyspace CqlIdentifier keyspaceId) {
    CqlSession unloggedSession = builder.build();
    assertThatThrownBy(() -> unloggedSession.execute("SELECT * FROM TEST WHERE k=1"))
        .isInstanceOf(InvalidQueryException.class)
        .hasMessage(
            "No keyspace has been specified. USE a keyspace, or explicitly specify keyspace.tablename");

    // Switch to keyspace and retry query
    unloggedSession.execute(String.format("USE %s", keyspaceId.asCql(false)));
    Row row = unloggedSession.execute("SELECT * FROM TEST WHERE k=1").one();
    assertThat(row).isNull();
    unloggedSession.close();
  }
}
