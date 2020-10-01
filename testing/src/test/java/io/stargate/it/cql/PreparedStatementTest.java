package io.stargate.it.cql;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import io.stargate.it.storage.ClusterConnectionInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PreparedStatementTest extends JavaDriverTestBase {

  public PreparedStatementTest(ClusterConnectionInfo backend) {
    super(backend);
  }

  @BeforeEach
  public void setupSchema() {
    session.execute("CREATE TABLE prepared_statement_test (a int PRIMARY KEY, b int, c int)");
  }

  @Test
  public void should_have_empty_result_definitions_for_insert_query_without_bound_variable() {
    PreparedStatement prepared =
        session.prepare("INSERT INTO prepared_statement_test (a, b, c) VALUES (1, 1, 1)");
    assertThat(prepared.getVariableDefinitions()).isEmpty();
    assertThat(prepared.getPartitionKeyIndices()).isEmpty();
    assertThat(prepared.getResultSetDefinitions()).isEmpty();
  }

  @Test
  public void should_have_non_empty_result_definitions_for_insert_query_with_bound_variable() {
    PreparedStatement prepared =
        session.prepare("INSERT INTO prepared_statement_test (a, b, c) VALUES (?, ?, ?)");
    assertAllColumns(prepared.getVariableDefinitions());
    assertThat(prepared.getPartitionKeyIndices()).containsExactly(0);
    assertThat(prepared.getResultSetDefinitions()).isEmpty();
  }

  @Test
  public void should_have_empty_variable_definitions_for_select_query_without_bound_variable() {
    PreparedStatement prepared =
        session.prepare("SELECT a,b,c FROM prepared_statement_test WHERE a = 1");
    assertThat(prepared.getVariableDefinitions()).isEmpty();
    assertThat(prepared.getPartitionKeyIndices()).isEmpty();
    assertAllColumns(prepared.getResultSetDefinitions());
  }

  @Test
  public void should_have_non_empty_variable_definitions_for_select_query_with_bound_variable() {
    PreparedStatement prepared =
        session.prepare("SELECT a,b,c FROM prepared_statement_test WHERE a = ?");
    assertThat(prepared.getVariableDefinitions()).hasSize(1);
    ColumnDefinition variable1 = prepared.getVariableDefinitions().get(0);
    assertThat(variable1.getKeyspace()).isEqualTo(keyspaceId);
    assertThat(variable1.getTable().asInternal()).isEqualTo("prepared_statement_test");
    assertThat(variable1.getName().asInternal()).isEqualTo("a");
    assertThat(variable1.getType()).isEqualTo(DataTypes.INT);
    assertThat(prepared.getPartitionKeyIndices()).containsExactly(0);
    assertAllColumns(prepared.getResultSetDefinitions());
  }

  private void assertAllColumns(ColumnDefinitions columnDefinitions) {
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
}
