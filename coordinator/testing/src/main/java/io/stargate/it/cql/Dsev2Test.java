package io.stargate.it.cql;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.protocol.internal.util.Bytes;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Covers the behavior of prepared statements when a table is altered (CASSANDRA-10786).
 *
 * <p>This test covers protocol-v5-specific features. However driver 4.9.0 is currently incompatible
 * with Cassandra 4.0 betas when forcing that version: the driver uses the new framing format from
 * CASSANDRA-15299, but that ticket is not merged yet on the server-side.
 *
 * <p>TODO reenable when CASSANDRA-15299 is merged
 */
@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    initQueries = {
      "CREATE KEYSPACE IF NOT EXISTS dse_v2 WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':'1'}"
    })
@EnabledIfSystemProperty(named = "stargate.enable.dsev2", matches = "true")
public class Dsev2Test extends BaseIntegrationTest {

  @BeforeEach
  public void setupSchema(CqlSession session) {
    // Recreate every time because the methods alter the schema
    session.execute(
        "CREATE TABLE IF NOT EXISTS dse_v2.statement_test (a int PRIMARY KEY, b int, c int)");
    session.execute(
        "CREATE TABLE IF NOT EXISTS dse_v2.batch_statement_test (a int PRIMARY KEY, b int, c int)");
    session.execute("INSERT INTO dse_v2.statement_test (a, b, c) VALUES (1, 1, 1)");
    session.execute("INSERT INTO dse_v2.statement_test (a, b, c) VALUES (2, 2, 2)");
    session.execute("INSERT INTO dse_v2.statement_test (a, b, c) VALUES (3, 3, 3)");
    session.execute("INSERT INTO dse_v2.statement_test (a, b, c) VALUES (4, 4, 4)");
  }

  @Test
  @DisplayName(
      "Should update prepared `SELECT *` metadata when table schema changes across executions")
  public void changeBetweenExecutionsTest(CqlSession session) {
    if (!backend.isDse()) return;
    // Given
    PreparedStatement ps =
        session.prepare(
            SimpleStatement.newInstance("SELECT * FROM statement_test WHERE a = ?")
                .setKeyspace("dse_v2"));

    ByteBuffer idBefore = ps.getResultMetadataId();

    // When
    session.execute("ALTER TABLE dse_v2.statement_test ADD d int");
    BoundStatement bs = ps.bind(1);
    ResultSet rows = session.execute(bs);

    // Then
    ByteBuffer idAfter = ps.getResultMetadataId();
    assertThat(Bytes.toHexString(idAfter)).isNotEqualTo(Bytes.toHexString(idBefore));
    for (ColumnDefinitions columnDefinitions :
        Arrays.asList(
            ps.getResultSetDefinitions(),
            bs.getPreparedStatement().getResultSetDefinitions(),
            rows.getColumnDefinitions())) {
      assertThat(columnDefinitions.get("d").getType()).isEqualTo(DataTypes.INT);
    }
  }

  @Test
  @DisplayName("Should update prepared `SELECT *` metadata when table schema changes across pages")
  public void changeBetweenPagesTest(CqlSession session) {
    if (!backend.isDse()) return;
    // Given
    PreparedStatement ps = session.prepare("SELECT * FROM dse_v2.statement_test");
    ByteBuffer idBefore = ps.getResultMetadataId();

    CompletionStage<AsyncResultSet> future = session.executeAsync(ps.bind().setPageSize(3));
    AsyncResultSet rows = CompletableFutures.getUninterruptibly(future);
    assertThat(rows.getColumnDefinitions().contains("e")).isFalse();
    // Consume the first page
    for (Row row : rows.currentPage()) {}

    // When
    session.execute("ALTER TABLE dse_v2.statement_test ADD e int");

    // Then
    // this should trigger a background fetch of the second page, and therefore update the
    // definitions
    assertThat(rows.hasMorePages()).isTrue();
    rows = CompletableFutures.getUninterruptibly(rows.fetchNextPage());
    for (Row row : rows.currentPage()) {
      assertThat(row.isNull("e")).isTrue();
    }
    assertThat(rows.getColumnDefinitions().get("e").getType()).isEqualTo(DataTypes.INT);
    // Should have updated the prepared statement too
    ByteBuffer idAfter = ps.getResultMetadataId();
    assertThat(Bytes.toHexString(idAfter)).isNotEqualTo(Bytes.toHexString(idBefore));
  }

  @Test
  @DisplayName("Should be able to process the select statement with keyspace name query time")
  public void setKeyspaceNameQueryTime(CqlSession session) {
    if (!backend.isDse()) return;
    // Given
    SimpleStatement statement =
        SimpleStatement.newInstance("SELECT * FROM statement_test").setKeyspace("dse_v2");
    final ResultSet resultSet = session.execute(statement);
    final List<Row> allRows = resultSet.all();
    assertThat(allRows.size()).isEqualTo(4);
  }

  @Test
  @DisplayName("Should be able to process the select statement with keyspace name query time")
  public void setKeyspaceNameBatch(CqlSession session) {
    if (!backend.isDse()) return;

    // When
    session.execute(
        BatchStatement.builder(DefaultBatchType.LOGGED)
            .setKeyspace("dse_v2")
            .addStatements(
                SimpleStatement.newInstance(
                    "INSERT INTO batch_statement_test (a, b, c) VALUES (1, 1, 1)"),
                SimpleStatement.newInstance(
                    "INSERT INTO batch_statement_test (a, b, c) VALUES (2, 2, 2)"))
            .build());
    // Given
    SimpleStatement statement =
        SimpleStatement.newInstance("SELECT * FROM batch_statement_test").setKeyspace("dse_v2");
    final ResultSet resultSet = session.execute(statement);
    final List<Row> allRows = resultSet.all();
    assertThat(allRows.size()).isEqualTo(2);
  }
}
