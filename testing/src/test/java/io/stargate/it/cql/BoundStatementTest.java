package io.stargate.it.cql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.QueryTrace;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.ByteUtils;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.servererrors.ProtocolError;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Note: all tests related to unset values assume that the protocol version is always 4 or higher.
 */
public class BoundStatementTest extends JavaDriverTestBase {

  private static final String KEY = "test";
  private static final int VALUE = 7;

  @BeforeEach
  public void setupSchema() {
    // table where every column forms the primary key.
    session.execute("CREATE TABLE IF NOT EXISTS test (k text, v int, PRIMARY KEY(k, v))");
    for (int i = 0; i < 100; i++) {
      session.execute(
          SimpleStatement.builder("INSERT INTO test (k, v) VALUES (?, ?)")
              .addPositionalValues(KEY, i)
              .build());
    }

    // table with simple primary key, single cell.
    session.execute("CREATE TABLE IF NOT EXISTS test2 (k text primary key, v0 int)");

    // table with composite partition key
    session.execute(
        "CREATE TABLE IF NOT EXISTS test3 "
            + "(pk1 int, pk2 int, v int, "
            + "PRIMARY KEY ((pk1, pk2)))");
  }

  @Test
  @DisplayName("Should execute statement with positional values")
  public void positionalValuesTest() {
    PreparedStatement prepared = session.prepare("INSERT INTO test2 (k, v0) values (?, ?)");
    session.execute(prepared.bind(KEY, VALUE));

    Row row = session.execute("SELECT v0 FROM test2 WHERE k=?", KEY).one();
    assertThat(row.getInt("v0")).isEqualTo(VALUE);
  }

  @Test
  @DisplayName("Should execute statement with named values")
  public void namedValuesTest() {
    PreparedStatement prepared = session.prepare("INSERT INTO test2 (k, v0) values (:k, :v)");
    session.execute(
        prepared.boundStatementBuilder().setString("k", KEY).setInt("v", VALUE).build());

    Row row = session.execute("SELECT v0 FROM test2 WHERE k=?", KEY).one();
    assertThat(row.getInt("v0")).isEqualTo(VALUE);
  }

  @Test
  @DisplayName("Should allow null values")
  public void nullValuesTest() {
    PreparedStatement prepared = session.prepare("INSERT INTO test2 (k, v0) values (?, ?)");
    session.execute(prepared.bind(KEY, null));

    Row row = session.execute("SELECT v0 FROM test2 WHERE k=?", KEY).one();
    assertThat(row.isNull("v0")).isTrue();
  }

  @Test
  @DisplayName("Should fail if a value is missing")
  public void missingValueTest() {
    PreparedStatement prepared = session.prepare("SELECT v FROM test3 WHERE pk1=? and pk2=?");
    assertThatThrownBy(() -> session.execute(prepared.bind(1)))
        .isInstanceOf(InvalidQueryException.class);
  }

  @Test
  @DisplayName("Should not write tombstone if value is implicitly unset")
  public void implicitUnsetTest() {
    PreparedStatement prepared = session.prepare("INSERT INTO test2 (k, v0) values (?, ?)");
    session.execute(prepared.bind(KEY, VALUE));

    BoundStatement boundStatement = prepared.bind(KEY);
    assertThat(boundStatement.isSet("v0")).isFalse();
    session.execute(boundStatement);

    Row row = session.execute("SELECT v0 FROM test2 WHERE k=?", KEY).one();
    assertThat(row.getInt("v0")).isEqualTo(VALUE);
  }

  @Test
  @DisplayName("Should not write tombstone if value is explicitly unset")
  public void explicitUnsetTest() {
    PreparedStatement prepared = session.prepare("INSERT INTO test2 (k, v0) values (?, ?)");
    BoundStatement boundStatement = prepared.bind(KEY, VALUE);
    session.execute(boundStatement);

    session.execute(boundStatement.unset("v0"));

    Row row = session.execute("SELECT v0 FROM test2 WHERE k=?", KEY).one();
    assertThat(row.getInt("v0")).isEqualTo(VALUE);
  }

  @Test
  @DisplayName("Should execute statement with custom page size")
  public void pageSizeTest() {
    PreparedStatement prepared =
        session.prepare(
            SimpleStatement.newInstance("SELECT v FROM test WHERE k=?").setPageSize(20));
    BoundStatement statement = prepared.bind(KEY);
    ResultSet resultSet = session.execute(statement);
    assertThat(resultSet.getAvailableWithoutFetching()).isEqualTo(20);

    statement = statement.copy(resultSet.getExecutionInfo().getPagingState());
    resultSet = session.execute(statement);

    assertThat(resultSet.iterator().next().getInt("v")).isEqualTo(20);
  }

  @Test
  @DisplayName("Should fail if the paging state is corrupted")
  public void corruptPagingStateTest() {
    PreparedStatement prepared = session.prepare("SELECT v FROM test WHERE k=?");
    BoundStatement statement =
        prepared
            .boundStatementBuilder(KEY)
            .setPagingState(ByteUtils.fromHexString("0x1234"))
            .build();
    assertThatThrownBy(() -> session.execute(statement)).isInstanceOf(ProtocolError.class);
  }

  @Test
  @DisplayName("Should execute statement with custom query timestamp")
  public void queryTimestampTest() {
    long timestamp = 10; // whatever
    session.execute(
        SimpleStatement.builder("INSERT INTO test2 (k, v0) values ('test', 1)")
            .setQueryTimestamp(timestamp)
            .build());

    Row row = session.execute("SELECT writetime(v0) FROM test2 WHERE k = 'test'").one();
    assertThat(row).isNotNull();
    assertThat(row.getLong(0)).isEqualTo(timestamp);
  }

  @Test
  @DisplayName("Should execute statement with tracing and retrieve trace")
  public void tracingTest() {
    PreparedStatement prepared = session.prepare("SELECT v FROM test WHERE k=?");
    BoundStatement statement = prepared.bind(KEY);

    ExecutionInfo executionInfo = session.execute(statement).getExecutionInfo();
    assertThat(executionInfo.getTracingId()).isNull();

    executionInfo = session.execute(statement.setTracing(true)).getExecutionInfo();
    assertThat(executionInfo.getTracingId()).isNotNull();
    QueryTrace queryTrace = executionInfo.getQueryTrace();
    assertThat(queryTrace).isNotNull();
    assertThat(queryTrace.getCoordinatorAddress().getAddress())
        .isIn(getStargateInetSocketAddresses());
    assertThat(queryTrace.getRequestType()).isEqualTo("Execute CQL3 prepared query");
    assertThat(queryTrace.getEvents()).isNotEmpty();
  }

  @Test
  @DisplayName("Should use statement-level consistency levels")
  public void consistencyLevelsTest() {
    PreparedStatement prepared = session.prepare("SELECT v FROM test WHERE k=?");
    BoundStatement statement = prepared.bind(KEY).setTracing(true);
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
}
