package io.stargate.it.cql;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import java.util.function.Function;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * This test covers protocol-v5-specific features. However driver 4.9.0 is currently incompatible
 * with Cassandra 4.0 betas when forcing that version: the driver uses the new framing format from
 * CASSANDRA-15299, but that ticket is not merged yet on the server-side.
 *
 * <p>TODO reenable when CASSANDRA-15299 is merged
 */
@Disabled("Requires CASSANDRA-15299 on the backend")
@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    customOptions = "enableProtocolV5",
    initQueries = "CREATE TABLE test(k int PRIMARY KEY, v int)")
public class NowInSecondsTest extends BaseIntegrationTest {

  public static void enableProtocolV5(OptionsMap config) {
    config.put(TypedDriverOption.PROTOCOL_VERSION, "V5");
  }

  @Test
  @DisplayName("Should use setNowInSeconds() with simple statement")
  @EnabledIf("isCassandra4")
  public void simpleStatementTest(CqlSession session) {
    should_use_now_in_seconds(SimpleStatement::newInstance, session);
  }

  @Test
  @EnabledIf("isCassandra4")
  @DisplayName("Should use setNowInSeconds() with bound statement")
  public void boundStatementTest(CqlSession session) {
    should_use_now_in_seconds(
        queryString -> {
          PreparedStatement preparedStatement = session.prepare(queryString);
          return preparedStatement.bind();
        },
        session);
  }

  @Test
  @DisplayName("Should use setNowInSeconds() with batch statement")
  @EnabledIf("isCassandra4")
  public void batchStatementTest(CqlSession session) {
    should_use_now_in_seconds(
        queryString ->
            BatchStatement.newInstance(BatchType.LOGGED, SimpleStatement.newInstance(queryString)),
        session);
  }

  private <StatementT extends Statement<StatementT>> void should_use_now_in_seconds(
      Function<String, StatementT> buildWriteStatement, CqlSession session) {
    // Given
    StatementT writeStatement =
        buildWriteStatement.apply("INSERT INTO test (k,v) VALUES (1,1) USING TTL 20");
    SimpleStatement readStatement =
        SimpleStatement.newInstance("SELECT TTL(v) FROM test WHERE k = 1");

    // When
    // insert at t = 0 with TTL 20
    session.execute(writeStatement.setNowInSeconds(0));
    // read TTL at t = 10
    ResultSet rs = session.execute(readStatement.setNowInSeconds(10));
    int remainingTtl = rs.one().getInt(0);

    // Then
    assertThat(remainingTtl).isEqualTo(10);
  }
}
