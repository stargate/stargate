package io.stargate.it.cql;

import static io.stargate.it.cql.SetKeyspaceTestUtil.testSetKeyspace;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.protocol.internal.util.Bytes;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.driver.WithProtocolVersion;
import java.nio.ByteBuffer;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
public abstract class PreparedStatementTest extends BaseIntegrationTest {

  @BeforeEach
  public void createSchema(CqlSession session) {
    // Must recreate every time because some methods alter the schema
    session.execute("DROP TABLE IF EXISTS prepared_statement_test");
    session.execute("CREATE TABLE prepared_statement_test (a int PRIMARY KEY, b int, c int)");
  }

  @Test
  @DisplayName("Should get expected metadata when preparing INSERT with no variables")
  public void insertWithoutVariablesTest(CqlSession session) {
    PreparedStatement prepared =
        session.prepare("INSERT INTO prepared_statement_test (a, b, c) VALUES (1, 1, 1)");
    assertThat(prepared.getVariableDefinitions()).isEmpty();
    assertThat(prepared.getPartitionKeyIndices()).isEmpty();
    assertThat(prepared.getResultSetDefinitions()).isEmpty();
  }

  @Test
  @DisplayName("Should get expected metadata when preparing INSERT with variables")
  public void insertWithVariablesTest(CqlSession session, @TestKeyspace CqlIdentifier keyspaceId) {
    PreparedStatement prepared =
        session.prepare("INSERT INTO prepared_statement_test (a, b, c) VALUES (?, ?, ?)");
    assertAllColumns(prepared.getVariableDefinitions(), keyspaceId);
    assertThat(prepared.getPartitionKeyIndices()).containsExactly(0);
    assertThat(prepared.getResultSetDefinitions()).isEmpty();
  }

  @Test
  @DisplayName("Should get expected metadata when preparing SELECT without variables")
  public void selectWithoutVariablesTest(
      CqlSession session, @TestKeyspace CqlIdentifier keyspaceId) {
    PreparedStatement prepared =
        session.prepare("SELECT a,b,c FROM prepared_statement_test WHERE a = 1");
    assertThat(prepared.getVariableDefinitions()).isEmpty();
    assertThat(prepared.getPartitionKeyIndices()).isEmpty();
    assertAllColumns(prepared.getResultSetDefinitions(), keyspaceId);
  }

  @Test
  @DisplayName("Should get expected metadata when preparing SELECT with variables")
  public void selectWithVariablesTest(CqlSession session, @TestKeyspace CqlIdentifier keyspaceId) {
    PreparedStatement prepared =
        session.prepare("SELECT a,b,c FROM prepared_statement_test WHERE a = ?");
    assertThat(prepared.getVariableDefinitions()).hasSize(1);
    ColumnDefinition variable1 = prepared.getVariableDefinitions().get(0);
    assertThat(variable1.getKeyspace()).isEqualTo(keyspaceId);
    assertThat(variable1.getTable().asInternal()).isEqualTo("prepared_statement_test");
    assertThat(variable1.getName().asInternal()).isEqualTo("a");
    assertThat(variable1.getType()).isEqualTo(DataTypes.INT);
    assertThat(prepared.getPartitionKeyIndices()).containsExactly(0);
    assertAllColumns(prepared.getResultSetDefinitions(), keyspaceId);
  }

  @Test
  @DisplayName("Should fail to reprepare if the query becomes invalid after a schema change")
  public void failedReprepareTest(CqlSession session) {
    // Given
    session.execute("ALTER TABLE prepared_statement_test ADD d int");
    PreparedStatement ps =
        session.prepare("SELECT a, b, c, d FROM prepared_statement_test WHERE a = ?");
    session.execute("ALTER TABLE prepared_statement_test DROP d");

    assertThatThrownBy(() -> session.execute(ps.bind()))
        .isInstanceOf(InvalidQueryException.class)
        .hasMessageContaining("Undefined column name d");
  }

  @Test
  @DisplayName("Should not store metadata for conditional updates")
  public void conditionalUpdateTest(CqlSession session) {
    // Given
    PreparedStatement ps =
        session.prepare(
            "INSERT INTO prepared_statement_test (a, b, c) VALUES (?, ?, ?) IF NOT EXISTS");

    // Never store metadata in the prepared statement for conditional updates, since the result set
    // can change
    // depending on the outcome.
    assertThat(ps.getResultSetDefinitions()).hasSize(0);
    ByteBuffer idBefore = ps.getResultMetadataId();

    // When
    ResultSet rs = session.execute(ps.bind(5, 5, 5));

    // Then
    // Successful conditional update => only contains the [applied] column
    assertThat(rs.wasApplied()).isTrue();
    assertThat(rs.getColumnDefinitions()).hasSize(1);
    assertThat(rs.getColumnDefinitions().get("[applied]").getType()).isEqualTo(DataTypes.BOOLEAN);
    // However the prepared statement shouldn't have changed
    assertThat(ps.getResultSetDefinitions()).hasSize(0);
    assertThat(Bytes.toHexString(ps.getResultMetadataId())).isEqualTo(Bytes.toHexString(idBefore));

    // When
    rs = session.execute(ps.bind(5, 5, 5));

    // Then
    // Failed conditional update => regular metadata
    assertThat(rs.wasApplied()).isFalse();
    assertThat(rs.getColumnDefinitions()).hasSize(4);
    Row row = rs.one();
    assertThat(row.getBoolean("[applied]")).isFalse();
    assertThat(row.getInt("a")).isEqualTo(5);
    assertThat(row.getInt("b")).isEqualTo(5);
    assertThat(row.getInt("c")).isEqualTo(5);
    // The prepared statement still shouldn't have changed
    assertThat(ps.getResultSetDefinitions()).hasSize(0);
    assertThat(Bytes.toHexString(ps.getResultMetadataId())).isEqualTo(Bytes.toHexString(idBefore));

    // When
    session.execute("ALTER TABLE prepared_statement_test ADD d int");
    rs = session.execute(ps.bind(5, 5, 5));

    // Then
    // Failed conditional update => regular metadata that should also contain the new column
    assertThat(rs.wasApplied()).isFalse();
    assertThat(rs.getColumnDefinitions()).hasSize(5);
    row = rs.one();
    assertThat(row.getBoolean("[applied]")).isFalse();
    assertThat(row.getInt("a")).isEqualTo(5);
    assertThat(row.getInt("b")).isEqualTo(5);
    assertThat(row.getInt("c")).isEqualTo(5);
    assertThat(row.isNull("d")).isTrue();
    assertThat(ps.getResultSetDefinitions()).hasSize(0);
    assertThat(Bytes.toHexString(ps.getResultMetadataId())).isEqualTo(Bytes.toHexString(idBefore));
  }

  @Test
  @DisplayName("Should return just one selected column, not more")
  public void noExtraValuesTest(CqlSession session) {
    // table with composite key
    session.execute(
        "CREATE TABLE IF NOT EXISTS no_extra_values_test (k int, c1 int, c2 int, v int, PRIMARY KEY (k, c1, c2))");
    try {
      session.execute("INSERT INTO no_extra_values_test (k, c1, c2, v) VALUES (1, 1, 2, 42)");
      PreparedStatement ps =
          session.prepare("SELECT v FROM no_extra_values_test WHERE k IN (1, 0) ORDER BY c1 ");
      // IMPORTANT! Must prevent paging, otherwise we'll error for other reasons
      ResultSet resultSet = session.execute(ps.bind().setPageSize(Integer.MAX_VALUE));
      assertThat(resultSet.getColumnDefinitions().size()).isEqualTo(1);
      assertThat(resultSet.getColumnDefinitions().get(0).getName().toString()).isEqualTo("v");
      List<Row> rows = resultSet.all();
      assertThat(rows).hasSize(1);
      Row row = rows.get(0);
      assertThat(row.size()).isEqualTo(1);
      assertThat(row.getColumnDefinitions()).hasSize(1);
      assertThat(row.getInt("v")).isEqualTo(42);
    } finally {
      session.execute("DROP TABLE IF EXISTS no_extra_values_test");
    }
  }

  private void assertAllColumns(ColumnDefinitions columnDefinitions, CqlIdentifier keyspaceId) {
    assertThat(columnDefinitions).hasSize(3);
    ColumnDefinition column1 = columnDefinitions.get(0);
    assertThat(column1.getKeyspace()).isEqualTo(keyspaceId);
    assertThat(column1.getTable().asInternal()).isEqualTo("prepared_statement_test");
    assertThat(column1.getName().asInternal()).isEqualTo("a");
    assertThat(column1.getType()).isEqualTo(DataTypes.INT);
    ColumnDefinition column2 = columnDefinitions.get(1);
    assertThat(column2.getKeyspace()).isEqualTo(keyspaceId);
    assertThat(column2.getTable().asInternal()).isEqualTo("prepared_statement_test");
    assertThat(column2.getName().asInternal()).isEqualTo("b");
    assertThat(column2.getType()).isEqualTo(DataTypes.INT);
    ColumnDefinition column3 = columnDefinitions.get(2);
    assertThat(column3.getKeyspace()).isEqualTo(keyspaceId);
    assertThat(column3.getTable().asInternal()).isEqualTo("prepared_statement_test");
    assertThat(column3.getName().asInternal()).isEqualTo("c");
    assertThat(column3.getType()).isEqualTo(DataTypes.INT);
  }

  @WithProtocolVersion("V4")
  public static class WithV4ProtocolVersionTest extends PreparedStatementTest {}

  @WithProtocolVersion("V5")
  public static class WithV5ProtocolVersionTest extends PreparedStatementTest {

    @Test
    @DisplayName("Should be able to set keyspace in prepare statement")
    public void setKeyspaceTest(CqlSession session, TestInfo testInfo) {
      testSetKeyspace(
          session,
          testInfo,
          1,
          (ks1, ks2) -> {
            // Prepare statements
            session.execute("USE " + ks1);
            BoundStatement stmt =
                session.prepare("INSERT INTO tab (k, v) VALUES (?, ?)").bind(1, 1);
            session.execute(stmt);

            stmt =
                session
                    .prepare(
                        SimpleStatement.newInstance("INSERT INTO tab (k, v) VALUES (?, ?)")
                            .setKeyspace(ks2))
                    .bind(2, 2);
            session.execute(stmt);
          });
    }

    @Test
    public void resultSetMetadataIdTest(CqlSession session) {
      PreparedStatement prepared =
          session.prepare("INSERT INTO prepared_statement_test (a, b, c) VALUES (1, 1, 1)");
      ByteBuffer id = prepared.getResultMetadataId();
      assertThat(id).isNotNull();
      assertThat(id.hasRemaining()).isTrue();
    }
  }
}
