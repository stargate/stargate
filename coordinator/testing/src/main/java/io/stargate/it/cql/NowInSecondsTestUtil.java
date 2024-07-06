package io.stargate.it.cql;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import java.util.function.Function;

/** This test covers protocol-v5-specific features. */
public class NowInSecondsTestUtil {
  static <StatementT extends Statement<StatementT>> void testNowInSeconds(
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
