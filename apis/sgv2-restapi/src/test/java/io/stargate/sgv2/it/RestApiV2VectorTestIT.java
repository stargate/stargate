package io.stargate.sgv2.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.api.common.exception.model.dto.ApiError;
import io.stargate.sgv2.common.IntegrationTestUtils;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import io.stargate.sgv2.restapi.service.models.Sgv2ColumnDefinition;
import io.stargate.sgv2.restapi.service.models.Sgv2Table;
import io.stargate.sgv2.restapi.service.models.Sgv2TableAddRequest;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@QuarkusTestResource(StargateTestResource.class)
public class RestApiV2VectorTestIT extends RestApiV2QIntegrationTestBase {
  public RestApiV2VectorTestIT() {
    super("vec_ks_", "vec_t_", KeyspaceCreation.PER_CLASS);
  }

  @BeforeAll
  public static void validateRunningOnVSearchEnabled() {
    assumeThat(IntegrationTestUtils.supportsVSearch())
        .as("Test disabled if backend does not support Vector Search (vsearch)")
        .isTrue();
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Tests: Create with Vector, index
  /////////////////////////////////////////////////////////////////////////
   */

  @Test
  public void tableCreateWithVectorIndex() {
    final String tableName = testTableName();
    createVectorTable(testKeyspaceName(), tableName, "vector<float, 5>");
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Tests: Create, fail
  /////////////////////////////////////////////////////////////////////////
   */

  // TODO: Add test trying to create vector column with non-float element
  //   type like "vector<string, 5>
  @Test
  @Disabled("Dse-next backend does not yet seem to validate vector element type")
  public void tableCreateFailForNonFloatType() {
    final String keyspace = testKeyspaceName();
    final String tableName = testTableName();

    // Inlined "createVectorTable" so we can modify call appropriately
    final List<Sgv2ColumnDefinition> columnDefs =
        Arrays.asList(
            new Sgv2ColumnDefinition("id", "int", false),
            new Sgv2ColumnDefinition("embedding", "vector<int, 4>", false));
    Sgv2Table.PrimaryKey primaryKey = new Sgv2Table.PrimaryKey(Arrays.asList("id"), null);
    final Sgv2TableAddRequest tableAdd =
        new Sgv2TableAddRequest(tableName, primaryKey, columnDefs, false, null);
    String response = tryCreateTable(keyspace, tableAdd, HttpStatus.SC_BAD_REQUEST);
    ApiError apiError = readJsonAs(response, ApiError.class);
    assertThat(apiError.code()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(apiError.description())
        .contains("INVALID_ARGUMENT")
        .contains("vectors may only use float. given int");
  }

  // TODO: Add test trying to create "too big" vector column
  // 24-Aug-2021, tatu: Current version of dse-next does not have guard rails yet,
  //    cannot yet add test
  @Test
  @Disabled("Cannot test 'too big vector' before dse-next backend version supports it")
  public void tableCreateFailForTooBigVector() {
    final String keyspace = testKeyspaceName();
    final String tableName = testTableName();

    // Inlined "createVectorTable" so we can modify call appropriately
    final List<Sgv2ColumnDefinition> columnDefs =
        Arrays.asList(
            new Sgv2ColumnDefinition("id", "int", false),
            new Sgv2ColumnDefinition("embedding", "vector<float, 99999>", false));
    Sgv2Table.PrimaryKey primaryKey = new Sgv2Table.PrimaryKey(Arrays.asList("id"), null);
    final Sgv2TableAddRequest tableAdd =
        new Sgv2TableAddRequest(tableName, primaryKey, columnDefs, false, null);
    String response = tryCreateTable(keyspace, tableAdd, HttpStatus.SC_BAD_REQUEST);
    ApiError apiError = readJsonAs(response, ApiError.class);
    assertThat(apiError.code()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(apiError.description())
        .contains("INVALID_ARGUMENT")
        // TODO: verify actual failure message, this is just a placeholder
        .contains("exceeds maximum vector length");
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Tests: INSERT, GET Row(s), happy
  /////////////////////////////////////////////////////////////////////////
   */

  @Test
  public void insertRowWithVectorValue() {
    final String tableName = testTableName();
    createVectorTable(testKeyspaceName(), tableName, "vector<float, 5>");

    insertTypedRows(
        testKeyspaceName(),
        tableName,
        Arrays.asList(
            map("id", 1, "embedding", Arrays.asList(0.0, 0.0, 0.25, 0.0, 0.0)),
            map("id", 2, "embedding", Arrays.asList(0.5, 0.5, 0.5, 0.5, 0.5)),
            map("id", 3, "embedding", Arrays.asList(1.0, 1.0, 1.0, 1.0, 0.875))));

    // And then select one of vector values
    List<Map<String, Object>> rows = findRowsAsList(testKeyspaceName(), tableName, 2);
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).get("id")).isEqualTo(2);
    // Challenge here is that Jackson decodes fp values as Double, need to account for that
    assertThat(rows.get(0).get("embedding")).isEqualTo(Arrays.asList(0.5, 0.5, 0.5, 0.5, 0.5));
  }

  @Test
  public void insertRowWithoutVectorValue() {
    final String tableName = testTableName();
    createVectorTable(testKeyspaceName(), tableName, "vector<float, 5>");

    insertTypedRows(
        testKeyspaceName(),
        tableName,
        Arrays.asList(
            map("id", 1, "embedding", Arrays.asList(0.0, 0.0, 0.25, 0.0, 0.0)),
            map("id", 2),
            map("id", 3)));

    // And then select one of vector values
    List<Map<String, Object>> rows = findRowsAsList(testKeyspaceName(), tableName, 3);
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).get("id")).isEqualTo(3);
    // verify that we get null for the vector value
    assertThat(rows.get(0)).containsKey("embedding");
    assertThat(rows.get(0).get("embedding")).isNull();
  }

  @Test
  public void insertRowWithNullVectorValue() {
    final String tableName = testTableName();
    createVectorTable(testKeyspaceName(), tableName, "vector<float, 5>");

    insertTypedRows(
        testKeyspaceName(),
        tableName,
        Arrays.asList(
            map("id", 1, "embedding", Arrays.asList(0.0, 0.0, 0.25, 0.0, 0.0)),
            map("id", 5, "embedding", null)));

    // And then select one of vector values
    List<Map<String, Object>> rows = findRowsAsList(testKeyspaceName(), tableName, 5);
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).get("id")).isEqualTo(5);
    // verify that we get null for the vector value
    assertThat(rows.get(0)).containsKey("embedding");
    assertThat(rows.get(0).get("embedding")).isNull();
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Tests: INSERT, GET Row(s), fail
  /////////////////////////////////////////////////////////////////////////
   */

  @Test
  public void insertRowFailForWrongVectorLength() {
    final String tableName = testTableName();
    createVectorTable(testKeyspaceName(), tableName, "vector<float, 3>");

    Map<String, Object> row = new HashMap<>();
    row.put("id", 42);
    row.put("embedding", Arrays.asList(0.0, 0.25, 0.5, 0.75));

    String response =
        insertRowExpectStatus(testKeyspaceName(), tableName, row, HttpStatus.SC_BAD_REQUEST);
    ApiError error = readJsonAs(response, ApiError.class);
    assertThat(error.code()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(error.description()).contains("Expected vector of size 3, got 4");
  }

  @Test
  public void insertRowFailForWrongVectorElementType() {
    final String tableName = testTableName();
    createVectorTable(testKeyspaceName(), tableName, "vector<float, 3>");

    Map<String, Object> row = new HashMap<>();
    row.put("id", 42);
    row.put("embedding", Arrays.asList("a", "b", "c"));

    String response =
        insertRowExpectStatus(testKeyspaceName(), tableName, row, HttpStatus.SC_BAD_REQUEST);
    ApiError error = readJsonAs(response, ApiError.class);
    assertThat(error.code()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(error.description()).contains("Cannot coerce String");
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Tests: Delete
  /////////////////////////////////////////////////////////////////////////
   */

  // TODO: insert simple row with vector, delete it, check it's gone

  /*
  /////////////////////////////////////////////////////////////////////////
  // Helper methods
  /////////////////////////////////////////////////////////////////////////
   */

  private void createVectorTable(String keyspace, String tableName, String vectorDef) {
    final List<Sgv2ColumnDefinition> columnDefs =
        Arrays.asList(
            new Sgv2ColumnDefinition("id", "int", false),
            new Sgv2ColumnDefinition("embedding", vectorDef, false));
    Sgv2Table.PrimaryKey primaryKey = new Sgv2Table.PrimaryKey(Arrays.asList("id"), null);
    final Sgv2TableAddRequest tableAdd =
        new Sgv2TableAddRequest(tableName, primaryKey, columnDefs, false, null);
    NameResponse response = createTable(keyspace, tableAdd);
    assertThat(response.name).isEqualTo(tableName);

    // And then find the table itself
    final Sgv2Table table = findTable(keyspace, tableName);
    assertThat(table.name()).isEqualTo(tableName);

    assertThat(table.columnDefinitions()).hasSize(2);
    assertThat(table.columnDefinitions()).containsExactlyElementsOf(columnDefs);

    // Plus then SAI for vector field too
    createTestIndex(testKeyspaceName(), tableName, "embedding", "embedding_idx", true, null);
  }
}
