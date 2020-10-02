package io.stargate.it.cql;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import io.stargate.it.storage.ClusterConnectionInfo;
import java.util.List;
import org.junit.jupiter.api.Test;

public class WarningsTest extends JavaDriverTestBase {

  public WarningsTest(ClusterConnectionInfo backend) {
    super(backend);
  }

  @Test
  public void should_surface_query_warnings() {
    // Avoid using a system table
    session.execute("CREATE TABLE test_warnings (k0 text, k1 int, v int, PRIMARY KEY (k0, k1))");
    ResultSet resultSet = session.execute("SELECT count(*) FROM test_warnings");
    List<String> warnings = resultSet.getExecutionInfo().getWarnings();

    assertThat(warnings).containsExactly("Aggregation query used without partition key");
  }
}
