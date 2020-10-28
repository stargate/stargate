package io.stargate.it.cql;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

/**
 * This test covers protocol-v5-specific features. However driver 4.9.0 is currently incompatible
 * with Cassandra 4.0 betas when forcing that version: the driver uses the new framing format from
 * CASSANDRA-15299, but that ticket is not merged yet on the server-side.
 *
 * <p>TODO reenable when CASSANDRA-15299 is merged
 */
@Disabled("Requires CASSANDRA-15299 on the backend")
public class NowInSecondsTest extends JavaDriverTestBase {

  @Override
  protected void customizeConfig(OptionsMap config) {
    config.put(TypedDriverOption.PROTOCOL_VERSION, "V5");
  }

  @BeforeEach
  public void setupSchema() {
    session.execute("CREATE TABLE test(k int PRIMARY KEY, v int)");
  }

  @Test
  @DisplayName("Should use setNowInSeconds() with simple statement")
  @EnabledIf("isCassandra4")
  public void simpleStatementTest() {
    should_use_now_in_seconds(SimpleStatement::newInstance);
  }

  @Test
  @EnabledIf("isCassandra4")
  @DisplayName("Should use setNowInSeconds() with bound statement")
  public void boundStatementTest() {
    should_use_now_in_seconds(
        queryString -> {
          PreparedStatement preparedStatement = session.prepare(queryString);
          return preparedStatement.bind();
        });
  }

  @Test
  @DisplayName("Should use setNowInSeconds() with batch statement")
  @EnabledIf("isCassandra4")
  public void batchStatementTest() {
    should_use_now_in_seconds(
        queryString ->
            BatchStatement.newInstance(BatchType.LOGGED, SimpleStatement.newInstance(queryString)));
  }

  private <StatementT extends Statement<StatementT>> void should_use_now_in_seconds(
      Function<String, StatementT> buildWriteStatement) {
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
