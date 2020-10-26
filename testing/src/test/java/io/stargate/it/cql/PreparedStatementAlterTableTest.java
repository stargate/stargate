package io.stargate.it.cql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.google.common.collect.ImmutableList;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

/**
 * Covers the behavior of prepared statements when a table is altered (CASSANDRA-10786).
 *
 * <p>This test covers protocol-v5-specific features. However driver 4.9.0 is currently incompatible
 * with Cassandra 4.0 betas when forcing that version: the driver uses the new framing format from
 * CASSANDRA-15299, but that ticket is not merged yet on the server-side.
 *
 * <p>TODO reenable when CASSANDRA-15299 is merged
 */
@Disabled("Requires CASSANDRA-15299 on the backend")
public class PreparedStatementAlterTableTest extends JavaDriverTestBase {

  @Override
  protected void customizeConfig(OptionsMap config) {
    config.put(TypedDriverOption.PROTOCOL_VERSION, "V5");
    config.put(TypedDriverOption.REQUEST_PAGE_SIZE, 2);
  }

  @BeforeEach
  public void setupSchema() {
    session.execute("CREATE TABLE prepared_statement_test (a int PRIMARY KEY, b int, c int)");
    session.execute("INSERT INTO prepared_statement_test (a, b, c) VALUES (1, 1, 1)");
    session.execute("INSERT INTO prepared_statement_test (a, b, c) VALUES (2, 2, 2)");
    session.execute("INSERT INTO prepared_statement_test (a, b, c) VALUES (3, 3, 3)");
    session.execute("INSERT INTO prepared_statement_test (a, b, c) VALUES (4, 4, 4)");
  }

  @Test
  @DisplayName(
      "Should update prepared `SELECT *` metadata when table schema changes across executions")
  @EnabledIf("isCassandra4")
  public void changeBetweenExecutionsTest() {
    // Given
    PreparedStatement ps = session.prepare("SELECT * FROM prepared_statement_test WHERE a = ?");
    ByteBuffer idBefore = ps.getResultMetadataId();

    // When
    session.execute("ALTER TABLE prepared_statement_test ADD d int");
    BoundStatement bs = ps.bind(1);
    ResultSet rows = session.execute(bs);

    // Then
    ByteBuffer idAfter = ps.getResultMetadataId();
    assertThat(Bytes.toHexString(idAfter)).isNotEqualTo(Bytes.toHexString(idBefore));
    for (ColumnDefinitions columnDefinitions :
        ImmutableList.of(
            ps.getResultSetDefinitions(),
            bs.getPreparedStatement().getResultSetDefinitions(),
            rows.getColumnDefinitions())) {
      assertThat(columnDefinitions).hasSize(4);
      assertThat(columnDefinitions.get("d").getType()).isEqualTo(DataTypes.INT);
    }
  }

  @Test
  @DisplayName("Should update prepared `SELECT *` metadata when table schema changes across pages")
  @EnabledIf("isCassandra4")
  public void changeBetweenPagesTest() {
    // Given
    PreparedStatement ps = session.prepare("SELECT * FROM prepared_statement_test");
    ByteBuffer idBefore = ps.getResultMetadataId();
    assertThat(ps.getResultSetDefinitions()).hasSize(3);

    CompletionStage<AsyncResultSet> future = session.executeAsync(ps.bind());
    AsyncResultSet rows = CompletableFutures.getUninterruptibly(future);
    assertThat(rows.getColumnDefinitions()).hasSize(3);
    assertThat(rows.getColumnDefinitions().contains("d")).isFalse();
    // Consume the first page
    for (Row row : rows.currentPage()) {
      assertThatThrownBy(() -> row.getInt("d")).isInstanceOf(IllegalArgumentException.class);
    }

    // When
    session.execute("ALTER TABLE prepared_statement_test ADD d int");

    // Then
    // this should trigger a background fetch of the second page, and therefore update the
    // definitions
    assertThat(rows.hasMorePages()).isTrue();
    rows = CompletableFutures.getUninterruptibly(rows.fetchNextPage());
    for (Row row : rows.currentPage()) {
      assertThat(row.isNull("d")).isTrue();
    }
    assertThat(rows.getColumnDefinitions()).hasSize(4);
    assertThat(rows.getColumnDefinitions().get("d").getType()).isEqualTo(DataTypes.INT);
    // Should have updated the prepared statement too
    ByteBuffer idAfter = ps.getResultMetadataId();
    assertThat(Bytes.toHexString(idAfter)).isNotEqualTo(Bytes.toHexString(idBefore));
    assertThat(ps.getResultSetDefinitions()).hasSize(4);
    assertThat(ps.getResultSetDefinitions().get("d").getType()).isEqualTo(DataTypes.INT);
  }
}
