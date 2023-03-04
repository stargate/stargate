package io.stargate.it.cql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
public abstract class AbstractCompressionTest extends BaseIntegrationTest {

  protected void compressionTest(CqlSession session) {

    session.execute("CREATE TABLE test (k text PRIMARY KEY, t text, i int, f float)");

    // Run a couple of simple test queries
    ResultSet rs =
        session.execute(
            SimpleStatement.newInstance(
                "INSERT INTO test (k, t, i, f) VALUES (?, ?, ?, ?)", "key", "foo", 42, 24.03f));
    assertThat(rs.iterator().hasNext()).isFalse();

    ResultSet rs1 = session.execute("SELECT * FROM test WHERE k = 'key'");
    assertThat(rs1.iterator().hasNext()).isTrue();
    Row row = rs1.iterator().next();
    assertThat(rs1.iterator().hasNext()).isFalse();
    assertThat(row.getString("k")).isEqualTo("key");
    assertThat(row.getString("t")).isEqualTo("foo");
    assertThat(row.getInt("i")).isEqualTo(42);
    assertThat(row.getFloat("f")).isEqualTo(24.03f, offset(0.1f));

    ExecutionInfo executionInfo = rs.getExecutionInfo();
    // There's not much more we can check without hard-coding sizes.
    // We are testing with small responses, so the compressed payload is not even guaranteed to be
    // smaller.
    assertThat(executionInfo.getResponseSizeInBytes()).isGreaterThan(0);
    assertThat(executionInfo.getCompressedResponseSizeInBytes()).isGreaterThan(0);
  }
}
