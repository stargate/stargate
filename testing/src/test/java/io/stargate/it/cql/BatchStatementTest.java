package io.stargate.it.cql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.testinfra.CassandraRequirement;
import io.stargate.it.storage.ClusterConnectionInfo;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@DisplayName("BatchStatementTest")
public class BatchStatementTest extends JavaDriverTestBase {

  private static final int batchCount = 100;

  public BatchStatementTest(ClusterConnectionInfo backend) {
    super(backend);
  }

  @Override
  protected void customizeConfig(OptionsMap config) {
    config.put(TypedDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30));
  }

  @BeforeEach
  public void createTables() {
    String[] schemaStatements =
        new String[] {
          "CREATE TABLE test (k0 text, k1 int, v int, PRIMARY KEY (k0, k1))",
          "CREATE TABLE counter1 (k0 text PRIMARY KEY, c counter)",
          "CREATE TABLE counter2 (k0 text PRIMARY KEY, c counter)",
          "CREATE TABLE counter3 (k0 text PRIMARY KEY, c counter)",
        };

    for (String schemaStatement : schemaStatements) {
      session.execute(SimpleStatement.newInstance(schemaStatement));
    }
  }

  @Test
  public void should_execute_batch_of_simple_statements_with_variables(TestInfo name) {
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

    verifyBatchInsert(name);
  }

  @Test
  public void should_execute_batch_of_bound_statements_with_variables(TestInfo name) {
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

    verifyBatchInsert(name);
  }

  @Test
  @CassandraRequirement(min = "2.2")
  public void should_execute_batch_of_bound_statements_with_unset_values(TestInfo name) {
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

    verifyBatchInsert(name);

    BatchStatementBuilder builder2 = BatchStatement.builder(DefaultBatchType.UNLOGGED);
    for (int i = 0; i < batchCount; i++) {
      BoundStatement boundStatement = preparedStatement.bind(i, i + 2);
      // unset v every 20 statements.
      if (i % 20 == 0) {
        boundStatement = boundStatement.unset(1);
      }
      builder.addStatement(boundStatement);
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
  public void should_execute_batch_of_bound_statements_with_named_variables(TestInfo name) {
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

    verifyBatchInsert(name);
  }

  @Test
  public void should_execute_batch_of_bound_and_simple_statements_with_variables(TestInfo name) {
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

    verifyBatchInsert(name);
  }

  @Test
  public void should_execute_cas_batch(TestInfo name) {
    // Build a batch with CAS operations on the same partition.
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

    verifyBatchInsert(name);

    // re execute same batch and ensure wasn't applied.
    result = session.execute(batchStatement);
    assertThat(result.wasApplied()).isFalse();
  }

  @Test
  public void should_execute_counter_batch(TestInfo name) {
    // should be able to do counter increments in a counter batch.
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

  private void doCounterIncrement(TestInfo name) {

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
  public void should_fail_logged_batch_with_counter_increment(TestInfo name) {
    // should not be able to do counter inserts in a unlogged batch.
    assertThatThrownBy(() -> doCounterIncrement(name)).isInstanceOf(InvalidQueryException.class);
  }

  private void doCounterAndNonCounterIncrement(TestInfo name) {

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
  public void should_fail_counter_batch_with_non_counter_increment(TestInfo name) {
    // should not be able to do a counter batch if it contains a non-counter increment statement.
    assertThatThrownBy(() -> doCounterAndNonCounterIncrement(name))
        .isInstanceOf(InvalidQueryException.class);
  }

  private void verifyBatchInsert(TestInfo name) {
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
}
