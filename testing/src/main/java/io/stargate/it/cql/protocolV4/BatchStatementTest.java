package io.stargate.it.cql.protocolV4;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.QueryTrace;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.storage.StargateConnectionInfo;
import io.stargate.it.storage.StargateEnvironmentInfo;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    initQueries = {
      "CREATE TABLE test (k0 text, k1 int, v int, PRIMARY KEY (k0, k1))",
      "CREATE TABLE counter1 (k0 text PRIMARY KEY, c counter)",
      "CREATE TABLE counter2 (k0 text PRIMARY KEY, c counter)",
      "CREATE TABLE counter3 (k0 text PRIMARY KEY, c counter)",
    },
    customOptions = "applyProtocolVersion")
public class BatchStatementTest extends BaseIntegrationTest {

  private static final int batchCount = 100;

  public static void applyProtocolVersion(OptionsMap config) {
    config.put(TypedDriverOption.PROTOCOL_VERSION, "V4");
  }

  @Test
  @DisplayName("Should execute batch of simple statements with variables")
  public void simpleStatementsVariablesTest(CqlSession session, TestInfo name) {
    // Build a batch of batchCount simple statements, each with their own positional variables.
    BatchStatementBuilder builder = BatchStatement.builder(DefaultBatchType.UNLOGGED);
    for (int i = 0; i < batchCount; i++) {
      SimpleStatement insert =
          SimpleStatement.builder(
                  String.format(
                      "INSERT INTO test (k0, k1, v) values ('%s', ?, ?)", name.getDisplayName()))
              .addPositionalValues(i, i + 1)
              .build();
      builder.addStatement(insert);
    }

    BatchStatement batchStatement = builder.build();
    session.execute(batchStatement);

    verifyBatchInsert(session, name);
  }

  @Test
  @DisplayName("Should execute batch of bound statements with variables")
  public void boundStatementsVariablesTest(CqlSession session, TestInfo name) {
    // Build a batch of batchCount statements with bound statements, each with their own positional
    // variables.
    BatchStatementBuilder builder = BatchStatement.builder(DefaultBatchType.UNLOGGED);
    SimpleStatement insert =
        SimpleStatement.builder(
                String.format(
                    "INSERT INTO test (k0, k1, v) values ('%s', ? , ?)", name.getDisplayName()))
            .build();
    PreparedStatement preparedStatement = session.prepare(insert);

    for (int i = 0; i < batchCount; i++) {
      builder.addStatement(preparedStatement.bind(i, i + 1));
    }

    BatchStatement batchStatement = builder.build();
    session.execute(batchStatement);

    verifyBatchInsert(session, name);
  }

  @Test
  @DisplayName("Should execute batch of bound statements with unset values")
  public void boundStatementsUnsetTest(CqlSession session, TestInfo name) {
    // Build a batch of batchCount statements with bound statements, each with their own positional
    // variables.
    BatchStatementBuilder builder = BatchStatement.builder(DefaultBatchType.UNLOGGED);
    SimpleStatement insert =
        SimpleStatement.builder(
                String.format(
                    "INSERT INTO test (k0, k1, v) values ('%s', ? , ?)", name.getDisplayName()))
            .build();
    PreparedStatement preparedStatement = session.prepare(insert);

    for (int i = 0; i < batchCount; i++) {
      builder.addStatement(preparedStatement.bind(i, i + 1));
    }

    BatchStatement batchStatement = builder.build();
    session.execute(batchStatement);

    verifyBatchInsert(session, name);

    BatchStatementBuilder builder2 = BatchStatement.builder(DefaultBatchType.UNLOGGED);
    for (int i = 0; i < batchCount; i++) {
      BoundStatement boundStatement = preparedStatement.bind(i, i + 2);
      // unset v every 20 statements.
      if (i % 20 == 0) {
        boundStatement = boundStatement.unset(1);
      }
      builder2.addStatement(boundStatement);
    }

    session.execute(builder2.build());

    Statement<?> select =
        SimpleStatement.builder("SELECT * from test where k0 = ?")
            .addPositionalValue(name.getDisplayName())
            .build();

    ResultSet result = session.execute(select);

    List<Row> rows = result.all();
    assertThat(rows).hasSize(100);

    Iterator<Row> iterator = rows.iterator();
    for (int i = 0; i < batchCount; i++) {
      Row row = iterator.next();
      assertThat(row.getString("k0")).isEqualTo(name.getDisplayName());
      assertThat(row.getInt("k1")).isEqualTo(i);
      // value should be from first insert (i + 1) if at row divisble by 20, otherwise second.
      int expectedValue = i % 20 == 0 ? i + 1 : i + 2;
      if (i % 20 == 0) {
        assertThat(row.getInt("v")).isEqualTo(expectedValue);
      }
    }
  }

  @Test
  @DisplayName("Should execute batch of bound statements with named variables")
  public void boundStatementsNamedVariablesTest(CqlSession session, TestInfo name) {
    // Build a batch of batchCount statements with bound statements, each with their own named
    // variable values.
    BatchStatementBuilder builder = BatchStatement.builder(DefaultBatchType.UNLOGGED);
    PreparedStatement preparedStatement =
        session.prepare("INSERT INTO test (k0, k1, v) values (:k0, :k1, :v)");

    for (int i = 0; i < batchCount; i++) {
      builder.addStatement(
          preparedStatement
              .boundStatementBuilder()
              .setString("k0", name.getDisplayName())
              .setInt("k1", i)
              .setInt("v", i + 1)
              .build());
    }

    BatchStatement batchStatement = builder.build();
    session.execute(batchStatement);

    verifyBatchInsert(session, name);
  }

  @Test
  @DisplayName("Should execute batch of bound and simple statements with variables")
  public void boundAndSimpleStatementsTest(CqlSession session, TestInfo name) {
    // Build a batch of batchCount statements with simple and bound statements alternating.
    BatchStatementBuilder builder = BatchStatement.builder(DefaultBatchType.UNLOGGED);
    SimpleStatement insert =
        SimpleStatement.builder(
                String.format(
                    "INSERT INTO test (k0, k1, v) values ('%s', ? , ?)", name.getDisplayName()))
            .build();
    PreparedStatement preparedStatement = session.prepare(insert);

    for (int i = 0; i < batchCount; i++) {
      if (i % 2 == 1) {
        SimpleStatement simpleInsert =
            SimpleStatement.builder(
                    String.format(
                        "INSERT INTO test (k0, k1, v) values ('%s', ?, ?)", name.getDisplayName()))
                .addPositionalValues(i, i + 1)
                .build();
        builder.addStatement(simpleInsert);
      } else {
        builder.addStatement(preparedStatement.bind(i, i + 1));
      }
    }

    BatchStatement batchStatement = builder.build();
    session.execute(batchStatement);

    verifyBatchInsert(session, name);
  }

  @Test
  @DisplayName("Should execute batch with multiple CAS operations on the same partition")
  public void casTest(CqlSession session, TestInfo name) {
    BatchStatementBuilder builder = BatchStatement.builder(DefaultBatchType.UNLOGGED);
    SimpleStatement insert =
        SimpleStatement.builder(
                String.format(
                    "INSERT INTO test (k0, k1, v) values ('%s', ? , ?) IF NOT EXISTS",
                    name.getDisplayName()))
            .build();
    PreparedStatement preparedStatement = session.prepare(insert);

    for (int i = 0; i < batchCount; i++) {
      builder.addStatement(preparedStatement.bind(i, i + 1));
    }

    BatchStatement batchStatement = builder.build();
    ResultSet result = session.execute(batchStatement);
    assertThat(result.wasApplied()).isTrue();

    verifyBatchInsert(session, name);

    // re execute same batch and ensure wasn't applied.
    result = session.execute(batchStatement);
    assertThat(result.wasApplied()).isFalse();
  }

  @Test
  @DisplayName("Should execute counter batch")
  public void counterTest(CqlSession session, TestInfo name) {
    assumeTrue(backend.supportsCounters(), "Test disabled if backend does not support counters()");
    BatchStatementBuilder builder = BatchStatement.builder(DefaultBatchType.COUNTER);

    for (int i = 1; i <= 3; i++) {
      SimpleStatement insert =
          SimpleStatement.builder(
                  String.format(
                      "UPDATE counter%d set c = c + %d where k0 = '%s'",
                      i, i, name.getDisplayName()))
              .build();
      builder.addStatement(insert);
    }

    BatchStatement batchStatement = builder.build();
    session.execute(batchStatement);

    for (int i = 1; i <= 3; i++) {
      ResultSet result =
          session.execute(
              String.format("SELECT c from counter%d where k0 = '%s'", i, name.getDisplayName()));

      List<Row> rows = result.all();
      assertThat(rows).hasSize(1);

      Row row = rows.iterator().next();
      assertThat(row.getLong("c")).isEqualTo(i);
    }
  }

  private void doCounterIncrement(CqlSession session, TestInfo name) {

    BatchStatementBuilder builder = BatchStatement.builder(DefaultBatchType.LOGGED);

    for (int i = 1; i <= 3; i++) {
      SimpleStatement insert =
          SimpleStatement.builder(
                  String.format(
                      "UPDATE counter%d set c = c + %d where k0 = '%s'",
                      i, i, name.getDisplayName()))
              .build();
      builder.addStatement(insert);
    }

    BatchStatement batchStatement = builder.build();
    session.execute(batchStatement);
  }

  @Test
  @DisplayName("Should fail if unlogged batch contains counter increments")
  public void unloggedCounterTest(CqlSession session, TestInfo name) {
    assertThatThrownBy(() -> doCounterIncrement(session, name))
        .isInstanceOf(InvalidQueryException.class);
  }

  private void doCounterAndNonCounterIncrement(CqlSession session, TestInfo name) {

    BatchStatementBuilder builder = BatchStatement.builder(DefaultBatchType.COUNTER);

    for (int i = 1; i <= 3; i++) {
      SimpleStatement insert =
          SimpleStatement.builder(
                  String.format(
                      "UPDATE counter%d set c = c + %d where k0 = '%s'",
                      i, i, name.getDisplayName()))
              .build();
      builder.addStatement(insert);
    }
    // add a non-counter increment statement.
    SimpleStatement simpleInsert =
        SimpleStatement.builder(
                String.format(
                    "INSERT INTO test (k0, k1, v) values ('%s', ?, ?)", name.getDisplayName()))
            .addPositionalValues(1, 2)
            .build();
    builder.addStatement(simpleInsert);

    BatchStatement batchStatement = builder.build();
    session.execute(batchStatement);
  }

  @Test
  @DisplayName("Should fail if counter batch contains non-counter operations")
  public void counterAndNoCounterTest(CqlSession session, TestInfo name) {
    assertThatThrownBy(() -> doCounterAndNonCounterIncrement(session, name))
        .isInstanceOf(InvalidQueryException.class);
  }

  private void verifyBatchInsert(CqlSession session, TestInfo name) {
    // validate data inserted by the batch.
    Statement<?> select =
        SimpleStatement.builder("SELECT * from test where k0 = ?")
            .addPositionalValue(name.getDisplayName())
            .build();

    ResultSet result = session.execute(select);

    List<Row> rows = result.all();
    assertThat(rows).hasSize(100);

    Iterator<Row> iterator = rows.iterator();
    for (int i = 0; i < batchCount; i++) {
      Row row = iterator.next();
      assertThat(row.getString("k0")).isEqualTo(name.getDisplayName());
      assertThat(row.getInt("k1")).isEqualTo(i);
      assertThat(row.getInt("v")).isEqualTo(i + 1);
    }
  }

  @Test
  @DisplayName("Should execute statement with tracing and retrieve trace")
  public void tracingTest(CqlSession session, TestInfo name, StargateEnvironmentInfo stargate) {
    // Build a batch of batchCount simple statements, each with their own positional variables.
    BatchStatementBuilder builder = BatchStatement.builder(DefaultBatchType.UNLOGGED);
    for (int i = 0; i < batchCount; i++) {
      SimpleStatement insert =
          SimpleStatement.builder(
                  String.format(
                      "INSERT INTO test (k0, k1, v) values ('%s', ?, ?)", name.getDisplayName()))
              .addPositionalValues(i, i + 1)
              .build();
      builder.addStatement(insert);
    }

    BatchStatement batchStatement = builder.setTracing().build();
    ResultSet resultSet = session.execute(batchStatement);

    QueryTrace queryTrace = resultSet.getExecutionInfo().getQueryTrace();
    assertThat(queryTrace).isNotNull();
    assertThat(stargate.nodes())
        .extracting(StargateConnectionInfo::seedAddress)
        .contains(queryTrace.getCoordinatorAddress().getAddress().getHostAddress());
    assertThat(queryTrace.getRequestType()).isEqualTo("Execute batch of CQL3 queries");
    assertThat(queryTrace.getEvents()).isNotEmpty();
  }
}
