package io.stargate.it.cql;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class WarningsTest extends JavaDriverTestBase {

  @Test
  @DisplayName("Should surface query warnings")
  public void warningsTest() {
    // Avoid using a system table
    session.execute("CREATE TABLE test_warnings (k0 text, k1 int, v int, PRIMARY KEY (k0, k1))");
    ResultSet resultSet = session.execute("SELECT count(*) FROM test_warnings");
    List<String> warnings = resultSet.getExecutionInfo().getWarnings();

    assertThat(warnings).containsExactly("Aggregation query used without partition key");
  }
}
