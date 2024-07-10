package io.stargate.it.cql;

import static io.stargate.it.cql.NowInSecondsTestUtil.testNowInSeconds;
import static io.stargate.it.cql.SetKeyspaceTestUtil.testSetKeyspace;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.data.ByteUtils;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.servererrors.ProtocolError;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.WithProtocolVersion;
import io.stargate.it.storage.StargateConnectionInfo;
import io.stargate.it.storage.StargateEnvironmentInfo;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    customOptions = "setPageSize",
    initQueries = {
      // table where every column forms the primary key.
      "CREATE TABLE IF NOT EXISTS test (k text, v int, PRIMARY KEY(k, v))",
      // table with simple primary key, single cell.
      "CREATE TABLE IF NOT EXISTS test2 (k text primary key, v int)",
      // table with composite key
      "CREATE TABLE IF NOT EXISTS test3 (k int, c1 int, c2 int, v int, PRIMARY KEY (k, c1, c2))"
    })
public abstract class SimpleStatementTest extends BaseIntegrationTest {

  private static final String KEY = "test";

  public static void setPageSize(OptionsMap config) {
    config.put(TypedDriverOption.REQUEST_PAGE_SIZE, 20);
  }

  @BeforeEach
  public void cleanupData(CqlSession session) {
    session.execute("TRUNCATE test");
    session.execute("TRUNCATE test2");
    session.execute("TRUNCATE test3");
    for (int i = 0; i < 100; i++) {
      session.execute("INSERT INTO test (k, v) VALUES (?, ?)", KEY, i);
    }
  }

  @Test
  @DisplayName("Should execute statement with positional values")
  public void positionalValuesTest(CqlSession session) {
    SimpleStatement statement = SimpleStatement.newInstance("SELECT v FROM test WHERE k=?", KEY);
    ResultSet resultSet = session.execute(statement);
    assertThat(resultSet).hasSize(100);
  }

  @Test
  @DisplayName("Should allow nulls in positional values")
  public void nullPositionalValuesTest(CqlSession session) {
    session.execute("INSERT into test2 (k, v) values (?, ?)", KEY, null);
    Row row = session.execute("select k,v from test2 where k=?", KEY).one();
    assertThat(row).isNotNull();
    assertThat(row.isNull("v")).isTrue();
  }

  @Test
  @DisplayName("Should fail when too many positional values are provided")
  public void tooManyPositionalValuesTest(CqlSession session) {
    assertThatThrownBy(
            () -> session.execute("INSERT into test2 (k, v) values (?, ?)", KEY, 1, 2, 3))
        .isInstanceOf(InvalidQueryException.class);
  }

  @Test
  @DisplayName("Should fail when not enough positional values are provided")
  public void notEnoughPositionalValuesTest(CqlSession session) {
    // For SELECT queries, all values must be filled
    assertThatThrownBy(() -> session.execute("SELECT * from test where k = ? and v = ?", KEY))
        .isInstanceOf(InvalidQueryException.class);
  }

  @Test
  @DisplayName("Should execute statement with named values")
  public void namedValuesTest(CqlSession session) {
    SimpleStatement statement =
        SimpleStatement.newInstance("SELECT v FROM test WHERE k=:k", ImmutableMap.of("k", KEY));
    ResultSet resultSet = session.execute(statement);
    assertThat(resultSet).hasSize(100);
  }

  @Test
  @DisplayName("Should allow nulls in names values")
  public void nullNamedValuesTest(CqlSession session) {
    session.execute(
        SimpleStatement.builder("INSERT into test2 (k, v) values (:k, :v)")
            .addNamedValue("k", KEY)
            .addNamedValue("v", null)
            .build());

    Row row = session.execute("select k,v from test2 where k=?", KEY).one();
    assertThat(row).isNotNull();
    assertThat(row.isNull("v")).isTrue();
  }

  @Test
  @DisplayName("Should fail if a named value is missing")
  public void missingNamedValueTest(CqlSession session) {
    // For SELECT queries, all values must be filled
    assertThatThrownBy(
            () ->
                session.execute(
                    SimpleStatement.newInstance(
                        "SELECT * from test where k = :k and v = :v", ImmutableMap.of("k", KEY))))
        .isInstanceOf(InvalidQueryException.class);
  }

  @Test
  @DisplayName("Should extract paging state from result and use it on another statement")
  public void pagingStateTest(CqlSession session) {
    SimpleStatement statement = SimpleStatement.newInstance("SELECT v FROM test WHERE k=?", KEY);
    ResultSet resultSet = session.execute(statement);
    assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(20);

    statement = statement.copy(resultSet.getExecutionInfo().getPagingState());
    resultSet = session.execute(statement);

    assertThat(resultSet.iterator().next().getInt("v")).isEqualTo(20);
  }

  @Test
  @DisplayName("Should fail if the paging state is corrupted")
  public void corruptPagingStateTest(CqlSession session) {
    SimpleStatement statement =
        SimpleStatement.builder("SELECT v FROM test WHERE k=?")
            .addPositionalValue(KEY)
            .setPagingState(ByteUtils.fromHexString("0x1234"))
            .build();
    assertThatThrownBy(() -> session.execute(statement)).isInstanceOf(ProtocolError.class);
  }

  @Test
  @DisplayName("Should execute statement with custom query timestamp")
  public void queryTimestampTest(CqlSession session) {
    long timestamp = 10; // whatever
    session.execute(
        SimpleStatement.builder("INSERT INTO test2 (k, v) values ('test', 1)")
            .setQueryTimestamp(timestamp)
            .build());

    Row row = session.execute("SELECT writetime(v) FROM test2 WHERE k = 'test'").one();
    assertThat(row).isNotNull();
    assertThat(row.getLong(0)).isEqualTo(timestamp);
  }

  @Test
  @DisplayName("Should execute statement with tracing and retrieve trace")
  public void tracingTest(CqlSession session, StargateEnvironmentInfo stargate) {
    SimpleStatement statement = SimpleStatement.newInstance("SELECT v FROM test WHERE k=?", KEY);

    ExecutionInfo executionInfo = session.execute(statement).getExecutionInfo();
    assertThat(executionInfo.getTracingId()).isNull();

    executionInfo = session.execute(statement.setTracing(true)).getExecutionInfo();
    assertThat(executionInfo.getTracingId()).isNotNull();
    QueryTrace queryTrace = executionInfo.getQueryTrace();
    assertThat(queryTrace).isNotNull();
    assertThat(stargate.nodes())
        .extracting(StargateConnectionInfo::seedAddress)
        .contains(queryTrace.getCoordinatorAddress().getAddress().getHostAddress());
    assertThat(queryTrace.getRequestType()).isEqualTo("Execute CQL3 query");
    assertThat(queryTrace.getEvents()).isNotEmpty();
  }

  @Test
  @DisplayName("Should execute statement with custom page size")
  public void pageSizeTest(CqlSession session) {
    SimpleStatement statement =
        SimpleStatement.newInstance("SELECT v FROM test WHERE k=?", KEY).setPageSize(10);
    ResultSet resultSet = session.execute(statement);
    assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(10);
  }

  @Test
  @DisplayName("Should use statement-level consistency levels")
  public void consistencyLevelsTest(CqlSession session) {
    SimpleStatement statement =
        SimpleStatement.newInstance("SELECT v FROM test WHERE k=?", KEY).setTracing(true);
    QueryTrace queryTrace = session.execute(statement).getExecutionInfo().getQueryTrace();
    // Driver defaults
    assertThat(queryTrace.getParameters().get("consistency_level")).isEqualTo("LOCAL_ONE");
    assertThat(queryTrace.getParameters().get("serial_consistency_level")).isEqualTo("SERIAL");

    statement =
        statement
            .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
            // Irrelevant for that query, but we just want to check that it gets reflected in the
            // trace
            .setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL);
    queryTrace = session.execute(statement).getExecutionInfo().getQueryTrace();
    assertThat(queryTrace.getParameters().get("consistency_level")).isEqualTo("LOCAL_QUORUM");
    assertThat(queryTrace.getParameters().get("serial_consistency_level"))
        .isEqualTo("LOCAL_SERIAL");
  }

  @Test
  @DisplayName("Should return just one selected column, not more")
  public void noExtraValuesTest(CqlSession session) {
    session.execute("INSERT INTO test3 (k, c1, c2, v) VALUES (1, 1, 2, 42)");
    SimpleStatement query =
        SimpleStatement.newInstance("SELECT v FROM test3 WHERE k IN (1, 0) ORDER BY c1 ")
            // IMPORTANT! Must prevent paging, otherwise we'll error for other reasons
            .setPageSize(Integer.MAX_VALUE);
    ResultSet resultSet = session.execute(query);
    assertThat(resultSet.getColumnDefinitions().size()).isEqualTo(1);
    assertThat(resultSet.getColumnDefinitions().get(0).getName().toString()).isEqualTo("v");
    List<Row> rows = resultSet.all();
    assertThat(rows).hasSize(1);
    Row row = rows.get(0);
    assertThat(row.size()).isEqualTo(1);
    assertThat(row.getColumnDefinitions()).hasSize(1);
    assertThat(row.getInt("v")).isEqualTo(42);
  }

  @WithProtocolVersion("V4")
  public static class WithV4ProtocolVersionTest extends SimpleStatementTest {}

  @WithProtocolVersion("V5")
  public static class WithV5ProtocolVersionTest extends SimpleStatementTest {
    @Test
    @DisplayName("Should use setKeyspace() with simple statement")
    public void setKeyspaceTest(CqlSession session, TestInfo testInfo) {
      testSetKeyspace(
          session,
          testInfo,
          1,
          (ks1, ks2) -> {
            // Prepare statements
            session.execute("USE " + ks1);
            session.execute(SimpleStatement.newInstance("INSERT INTO tab (k, v) VALUES (1, 1)"));

            session.execute(
                SimpleStatement.newInstance("INSERT INTO tab (k, v) VALUES (2, 2)")
                    .setKeyspace(ks2));
          });
    }

    @Test
    @DisplayName("Should use setNowInSeconds() with simple statement")
    public void nowInSecondsTest(CqlSession session) {
      testNowInSeconds(SimpleStatement::newInstance, session);
    }
  }
}
