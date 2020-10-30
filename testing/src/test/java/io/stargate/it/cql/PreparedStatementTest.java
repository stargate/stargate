package io.stargate.it.cql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.protocol.internal.util.Bytes;
import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.TestKeyspace;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
public class PreparedStatementTest extends BaseOsgiIntegrationTest {

  private String tableName;

  @BeforeEach
  public void createSchema(TestInfo testInfo, CqlSession session) {
    Optional<String> name = testInfo.getTestMethod().map(Method::getName);
    assertThat(name).isPresent();
    String testName = name.get();

    tableName = (testName + "_tbl").toLowerCase();
    // Must recreate every time because some methods alter the schema
    session.execute(String.format("DROP TABLE IF EXISTS %s", tableName));
    session.execute(String.format("CREATE TABLE %s (a int PRIMARY KEY, b int, c int)", tableName));
  }

  @Test
  @DisplayName("Should get expected metadata when preparing INSERT with no variables")
  public void insertWithoutVariablesTest(CqlSession session) {
    PreparedStatement prepared =
        session.prepare(String.format("INSERT INTO %s (a, b, c) VALUES (1, 1, 1)", tableName));
    assertThat(prepared.getVariableDefinitions()).isEmpty();
    assertThat(prepared.getPartitionKeyIndices()).isEmpty();
    assertThat(prepared.getResultSetDefinitions()).isEmpty();
  }

  @Test
  @DisplayName("Should get expected metadata when preparing INSERT with variables")
  public void insertWithVariablesTest(CqlSession session, @TestKeyspace CqlIdentifier keyspaceId) {
    PreparedStatement prepared =
        session.prepare(String.format("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", tableName));
    assertAllColumns(prepared.getVariableDefinitions(), keyspaceId);
    assertThat(prepared.getPartitionKeyIndices()).containsExactly(0);
    assertThat(prepared.getResultSetDefinitions()).isEmpty();
  }

  @Test
  @DisplayName("Should get expected metadata when preparing SELECT without variables")
  public void selectWithoutVariablesTest(
      CqlSession session, @TestKeyspace CqlIdentifier keyspaceId) {
    PreparedStatement prepared =
        session.prepare(String.format("SELECT a,b,c FROM %s WHERE a = 1", tableName));
    assertThat(prepared.getVariableDefinitions()).isEmpty();
    assertThat(prepared.getPartitionKeyIndices()).isEmpty();
    assertAllColumns(prepared.getResultSetDefinitions(), keyspaceId);
  }

  @Test
  @DisplayName("Should get expected metadata when preparing SELECT with variables")
  public void selectWithVariablesTest(CqlSession session, @TestKeyspace CqlIdentifier keyspaceId) {
    PreparedStatement prepared =
        session.prepare(String.format("SELECT a,b,c FROM %s WHERE a = ?", tableName));
    assertThat(prepared.getVariableDefinitions()).hasSize(1);
    ColumnDefinition variable1 = prepared.getVariableDefinitions().get(0);
    assertThat(variable1.getKeyspace()).isEqualTo(keyspaceId);
    assertThat(variable1.getTable().asInternal()).isEqualTo(tableName);
    assertThat(variable1.getName().asInternal()).isEqualTo("a");
    assertThat(variable1.getType()).isEqualTo(DataTypes.INT);
    assertThat(prepared.getPartitionKeyIndices()).containsExactly(0);
    assertAllColumns(prepared.getResultSetDefinitions(), keyspaceId);
  }

  @Test
  @DisplayName("Should fail to reprepare if the query becomes invalid after a schema change")
  public void failedReprepareTest(CqlSession session) {
    // Given
    session.execute(String.format("ALTER TABLE %s ADD d int", tableName));
    PreparedStatement ps =
        session.prepare(String.format("SELECT a, b, c, d FROM %s WHERE a = ?", tableName));
    session.execute(String.format("ALTER TABLE %s DROP d", tableName));

    assertThatThrownBy(() -> session.execute(ps.bind()))
        .isInstanceOf(InvalidQueryException.class)
        .hasMessage("Undefined column name d");
  }

  @Test
  @DisplayName("Should not store metadata for conditional updates")
  public void conditionalUpdateTest(CqlSession session) {
    // Given
    PreparedStatement ps =
        session.prepare(
            String.format("INSERT INTO %s (a, b, c) VALUES (?, ?, ?) IF NOT EXISTS", tableName));

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
    session.execute(String.format("ALTER TABLE %s ADD d int", tableName));
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

  private void assertAllColumns(ColumnDefinitions columnDefinitions, CqlIdentifier keyspaceId) {
    assertThat(columnDefinitions).hasSize(3);
    ColumnDefinition column1 = columnDefinitions.get(0);
    assertThat(column1.getKeyspace()).isEqualTo(keyspaceId);
    assertThat(column1.getTable().asInternal()).isEqualTo(tableName);
    assertThat(column1.getName().asInternal()).isEqualTo("a");
    assertThat(column1.getType()).isEqualTo(DataTypes.INT);
    ColumnDefinition column2 = columnDefinitions.get(1);
    assertThat(column2.getKeyspace()).isEqualTo(keyspaceId);
    assertThat(column2.getTable().asInternal()).isEqualTo(tableName);
    assertThat(column2.getName().asInternal()).isEqualTo("b");
    assertThat(column2.getType()).isEqualTo(DataTypes.INT);
    ColumnDefinition column3 = columnDefinitions.get(2);
    assertThat(column3.getKeyspace()).isEqualTo(keyspaceId);
    assertThat(column3.getTable().asInternal()).isEqualTo(tableName);
    assertThat(column3.getName().asInternal()).isEqualTo("c");
    assertThat(column3.getType()).isEqualTo(DataTypes.INT);
  }
}
