package io.stargate.it.cql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.stargate.it.storage.ClusterConnectionInfo;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Based off of HeapCompressionIT */
public class CompressionTest extends JavaDriverTestBase {

  static {
    System.setProperty("io.netty.noPreferDirect", "true");
    System.setProperty("io.netty.noUnsafe", "true");
  }

  public CompressionTest(ClusterConnectionInfo backend) {
    super(backend);
  }

  @Override
  protected void customizeConfig(OptionsMap config) {
    config.put(TypedDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30));
  }

  @BeforeEach
  public void setup() {
    session.execute("CREATE TABLE test (k text PRIMARY KEY, t text, i int, f float)");
  }

  /**
   * Validates that Snappy compression still works when using heap buffers.
   *
   * @test_category connection:compression
   * @expected_result session established and queries made successfully using it.
   */
  @Test
  public void should_execute_queries_with_snappy_compression() throws Exception {
    createAndCheckCluster("snappy", session);
  }

  /**
   * Validates that LZ4 compression still works when using heap buffers.
   *
   * @test_category connection:compression
   * @expected_result session established and queries made successfully using it.
   */
  @Test
  public void should_execute_queries_with_lz4_compression() throws Exception {
    createAndCheckCluster("lz4", session);
  }

  private void createAndCheckCluster(String compressorOption, CqlSession testSession) {
    Map<TypedDriverOption<String>, String> optionsToAdd =
        new HashMap<TypedDriverOption<String>, String>();
    optionsToAdd.put(TypedDriverOption.PROTOCOL_COMPRESSION, compressorOption);
    DriverConfigLoader loader = buildDriverConfigLoader(optionsToAdd);
    try (CqlSession session = buildSession(loader, Optional.of(keyspaceId))) {
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
}
