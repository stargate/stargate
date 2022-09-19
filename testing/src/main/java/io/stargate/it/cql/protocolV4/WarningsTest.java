package io.stargate.it.cql.protocolV4;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    initQueries = "CREATE TABLE test_warnings (k0 text, k1 int, v int, PRIMARY KEY (k0, k1))",
    customOptions = "applyProtocolVersion")
public class WarningsTest extends BaseIntegrationTest {
  public static void applyProtocolVersion(OptionsMap config) {
    config.put(TypedDriverOption.PROTOCOL_VERSION, "V4");
  }

  @Test
  @DisplayName("Should surface query warnings")
  public void warningsTest(CqlSession session) {
    ResultSet resultSet = session.execute("SELECT count(*) FROM test_warnings");
    List<String> warnings = resultSet.getExecutionInfo().getWarnings();

    assertThat(warnings).containsExactly("Aggregation query used without partition key");
  }
}
