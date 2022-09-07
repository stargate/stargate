package io.stargate.sgv2.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.ResourceArg;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.exception.model.dto.ApiError;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import io.stargate.sgv2.restapi.service.models.Sgv2ColumnDefinition;
import java.util.Arrays;
import java.util.List;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@QuarkusTestResource(
    value = StargateTestResource.class,
    initArgs =
        @ResourceArg(name = StargateTestResource.Options.DISABLE_FIXED_TOKEN, value = "true"))
public class RestApiV2QSchemaColumnsIT extends RestApiV2QIntegrationTestBase {
  public RestApiV2QSchemaColumnsIT() {
    super("col_ks_", "col_t_");
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Test methods, Add (Create)
  /////////////////////////////////////////////////////////////////////////
   */

  @Test
  public void columnAddBadType() {
    final String tableName = testTableName();
    createSimpleTestTable(testKeyspaceName(), tableName);

    final String columnToAdd = "name";
    Sgv2ColumnDefinition columnDef = new Sgv2ColumnDefinition(columnToAdd, "badType", false);
    String response =
        tryAddColumn(testKeyspaceName(), tableName, columnDef, HttpStatus.SC_BAD_REQUEST);
    ApiError apiError = readJsonAs(response, ApiError.class);
    assertThat(apiError.code()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    // Sub-optimal failure message, should improve:
    assertThat(apiError.description()).matches("Invalid argument.*Unknown type.*badType.*");
  }

  @Test
  public void columnAddBasic() {
    final String tableName = testTableName();
    createSimpleTestTable(testKeyspaceName(), tableName);

    // Add a column, verify response
    final String columnToAdd = "name";
    Sgv2ColumnDefinition columnDef = new Sgv2ColumnDefinition(columnToAdd, "text", false);
    String response = tryAddColumn(testKeyspaceName(), tableName, columnDef, HttpStatus.SC_CREATED);
    NameResponse nameResp = readJsonAs(response, NameResponse.class);
    assertThat(nameResp.name).isEqualTo(columnToAdd);

    Sgv2ColumnDefinition columnFound =
        findOneColumn(testKeyspaceName(), tableName, columnToAdd, true);
    assertThat(columnFound).isEqualTo(columnDef);
  }

  @Test
  public void columnAddStatic() {
    final String tableName = testTableName();
    setupClusteringTestCase(testKeyspaceName(), tableName);

    // Add a static column, verify response
    final String columnToAdd = "balance";
    Sgv2ColumnDefinition columnDef = new Sgv2ColumnDefinition(columnToAdd, "float", true);
    String response = tryAddColumn(testKeyspaceName(), tableName, columnDef, HttpStatus.SC_CREATED);
    NameResponse nameResp = readJsonAs(response, NameResponse.class);
    assertThat(nameResp.name).isEqualTo(columnToAdd);

    Sgv2ColumnDefinition columnFound =
        findOneColumn(testKeyspaceName(), tableName, columnToAdd, true);
    assertThat(columnFound).isEqualTo(columnDef);
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Test methods, Get single
  /////////////////////////////////////////////////////////////////////////
   */

  @Test
  public void columnGetBadKeyspace() {
    final String badKeyspace = "column-get-bad-keyspace-" + System.currentTimeMillis();
    String response = tryFindOneColumn(badKeyspace, "table", "column1", HttpStatus.SC_BAD_REQUEST);
    ApiError apiError = readJsonAs(response, ApiError.class);

    assertThat(apiError.code()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    // 02-Sep-2022, tatu: Error message is bit misleading but it is what it is; verify:
    assertThat(apiError.description())
        .matches("Table 'table' not found.*keyspace.*" + badKeyspace + ".*");
  }

  @Test
  public void columnGetBadTable() {
    String tableName = "column-bad-bogus-table";
    String response =
        tryFindOneColumn(testKeyspaceName(), tableName, "column1", HttpStatus.SC_BAD_REQUEST);
    ApiError apiError = readJsonAs(response, ApiError.class);
    assertThat(apiError.code()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(apiError.description()).matches("Table.*" + tableName + ".* not found.*");
  }

  @Test
  public void columnGetComplex() {
    final String tableName = testTableName();
    createComplexTestTable(testKeyspaceName(), tableName);

    Sgv2ColumnDefinition columnFound = findOneColumn(testKeyspaceName(), tableName, "col1", true);
    assertThat(columnFound)
        .isEqualTo(new Sgv2ColumnDefinition("col1", "frozen<map<date, text>>", false));
  }

  @Test
  public void columnGetNotFound() {
    final String tableName = testTableName();
    createSimpleTestTable(testKeyspaceName(), tableName);
    final String columnName = "column-get-not-found";
    String response =
        tryFindOneColumn(testKeyspaceName(), tableName, columnName, HttpStatus.SC_NOT_FOUND);
    ApiError apiError = readJsonAs(response, ApiError.class);
    assertThat(apiError.code()).isEqualTo(HttpStatus.SC_NOT_FOUND);
    assertThat(apiError.description()).matches("column.*" + columnName + ".* not found.*");
  }

  @Test
  public void columnGetRaw() {
    final String tableName = testTableName();
    createSimpleTestTable(testKeyspaceName(), tableName);

    Sgv2ColumnDefinition columnFound = findOneColumn(testKeyspaceName(), tableName, "age", true);
    assertThat(columnFound).isEqualTo(new Sgv2ColumnDefinition("age", "int", false));
  }

  @Test
  public void columnGetWrapped() {
    final String tableName = testTableName();
    createSimpleTestTable(testKeyspaceName(), tableName);

    Sgv2ColumnDefinition columnFound = findOneColumn(testKeyspaceName(), tableName, "age", false);
    assertThat(columnFound).isEqualTo(new Sgv2ColumnDefinition("age", "int", false));
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Test methods, Get multiple
  /////////////////////////////////////////////////////////////////////////
   */

  @Test
  public void columnsGetBadKeyspace() {
    final String badKeyspace = "columns-get-bad-keyspace-" + System.currentTimeMillis();
    String response = tryFindAllColumns(badKeyspace, "table", HttpStatus.SC_BAD_REQUEST);
    ApiError apiError = readJsonAs(response, ApiError.class);
    assertThat(apiError.code()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    // 02-Sep-2022, tatu: Error message is bit misleading but it is what it is; verify:
    assertThat(apiError.description())
        .matches("Table 'table' not found.*keyspace.*" + badKeyspace + ".*");
  }

  @Test
  public void columnsGetBadTable() {
    String tableName = "column-bad-bogus-table";
    String response = tryFindAllColumns(testKeyspaceName(), tableName, HttpStatus.SC_BAD_REQUEST);
    ApiError apiError = readJsonAs(response, ApiError.class);
    assertThat(apiError.code()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(apiError.description()).matches("Table.*" + tableName + ".* not found.*");
  }

  @Test
  public void columnsGetComplex() {
    final String tableName = testTableName();
    createComplexTestTable(testKeyspaceName(), tableName);

    Sgv2ColumnDefinition[] columnsFound = findAllColumns(testKeyspaceName(), tableName, true);
    List<Sgv2ColumnDefinition> columnsExcepted =
        Arrays.asList(
            new Sgv2ColumnDefinition("pk0", "uuid", false),
            new Sgv2ColumnDefinition("col1", "frozen<map<date, text>>", false),
            new Sgv2ColumnDefinition("col2", "frozen<set<boolean>>", false),
            new Sgv2ColumnDefinition("col3", "tuple<duration, inet>", false));

    assertThat(columnsFound).hasSize(columnsExcepted.size()).containsAll(columnsExcepted);
  }

  @Test
  public void columnsGetRaw() {
    final String tableName = testTableName();
    createSimpleTestTable(testKeyspaceName(), tableName);

    Sgv2ColumnDefinition[] columnsFound = findAllColumns(testKeyspaceName(), tableName, true);
    verifySimpleColumns(columnsFound);
  }

  @Test
  public void columnsGetWrapped() {
    final String tableName = testTableName();
    createSimpleTestTable(testKeyspaceName(), tableName);

    Sgv2ColumnDefinition[] columnsFound = findAllColumns(testKeyspaceName(), tableName, false);
    verifySimpleColumns(columnsFound);
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Test methods, Update
  /////////////////////////////////////////////////////////////////////////
   */

  @Test
  public void columnUpdate() {
    final String tableName = testTableName();
    createSimpleTestTable(testKeyspaceName(), tableName);
    final String oldColumnName = "id";
    final String newColumnName = "identifier";

    final Sgv2ColumnDefinition columnDef = new Sgv2ColumnDefinition(newColumnName, "uuid", false);
    String response =
        tryUpdateColumn(testKeyspaceName(), tableName, oldColumnName, columnDef, HttpStatus.SC_OK);
    assertThat(readJsonAs(response, NameResponse.class).name).isEqualTo(newColumnName);

    Sgv2ColumnDefinition columnFound =
        findOneColumn(testKeyspaceName(), tableName, newColumnName, true);
    assertThat(columnFound).isEqualTo(columnDef);
  }

  @Test
  public void columnUpdateBadKeyspace() {
    final String badKeyspace = "columns-update-bad-keyspace-" + System.currentTimeMillis();
    // Important! Old name and new name MUST be different, otherwise it's a no-op
    String response =
        tryUpdateColumn(
            badKeyspace,
            "table",
            "id",
            new Sgv2ColumnDefinition("id-renamed", "text", false),
            HttpStatus.SC_BAD_REQUEST);
    ApiError apiError = readJsonAs(response, ApiError.class);
    assertThat(apiError.code()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    // 02-Sep-2022, tatu: Error message is bit misleading but it is what it is; verify:
    assertThat(apiError.description())
        .matches("Table 'table' not found.*keyspace.*" + badKeyspace + ".*");
  }

  @Test
  public void columnUpdateBadTable() {
    final String tableName = "column-unknown-table";
    // Important! Old name and new name MUST be different, otherwise it's a no-op
    String response =
        tryUpdateColumn(
            testKeyspaceName(),
            tableName,
            "id",
            new Sgv2ColumnDefinition("id-renamed", "text", false),
            HttpStatus.SC_BAD_REQUEST);
    ApiError apiError = readJsonAs(response, ApiError.class);
    assertThat(apiError.code()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(apiError.description()).matches("Table .*" + tableName + ".* not found.*");
  }

  @Test
  public void columnUpdateNotFound() {
    final String tableName = testTableName();
    createSimpleTestTable(testKeyspaceName(), tableName);
    String response =
        tryUpdateColumn(
            testKeyspaceName(),
            tableName,
            "id0",
            new Sgv2ColumnDefinition("id-renamed", "text", false),
            HttpStatus.SC_BAD_REQUEST);
    ApiError apiError = readJsonAs(response, ApiError.class);
    assertThat(apiError.code()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(apiError.description()).matches("column.*id0.* not found in table.*");
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Test methods, Delete
  /////////////////////////////////////////////////////////////////////////
   */

  @Test
  public void columnDelete() {
    final String tableName = testTableName();
    createSimpleTestTable(testKeyspaceName(), tableName);
    final String columnName = "age";

    // First, verify we have all columns
    Sgv2ColumnDefinition[] columnsFound = findAllColumns(testKeyspaceName(), tableName, true);
    verifySimpleColumns(columnsFound);

    tryDeleteColumn(testKeyspaceName(), tableName, columnName, HttpStatus.SC_NO_CONTENT);

    columnsFound = findAllColumns(testKeyspaceName(), tableName, true);
    List<Sgv2ColumnDefinition> columnsExpected =
        Arrays.asList(
            new Sgv2ColumnDefinition("id", "uuid", false),
            new Sgv2ColumnDefinition("lastName", "text", false),
            new Sgv2ColumnDefinition("firstName", "text", false));
    assertThat(columnsFound).hasSize(3).containsAll(columnsExpected);
  }

  @Test
  public void columnDeleteBadKeyspace() {
    final String badKeyspace = "column-delete-bad-keyspace-" + System.currentTimeMillis();
    String response = tryDeleteColumn(badKeyspace, "table", "id", HttpStatus.SC_BAD_REQUEST);
    ApiError apiError = readJsonAs(response, ApiError.class);
    assertThat(apiError.code()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    // 02-Sep-2022, tatu: Error message is bit misleading but it is what it is; verify:
    assertThat(apiError.description())
        .matches("Table 'table' not found.*keyspace.*" + badKeyspace + ".*");
  }

  @Test
  public void columnDeleteBadTable() {
    final String tableName = "column-unknown-table";
    String response =
        tryDeleteColumn(testKeyspaceName(), tableName, "id", HttpStatus.SC_BAD_REQUEST);
    ApiError apiError = readJsonAs(response, ApiError.class);
    assertThat(apiError.code()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(apiError.description()).matches("Table .*" + tableName + ".* not found.*");
  }

  @Test
  public void columnDeleteNotFound() {
    final String tableName = testTableName();
    createSimpleTestTable(testKeyspaceName(), tableName);
    String response =
        tryDeleteColumn(testKeyspaceName(), tableName, "id0", HttpStatus.SC_BAD_REQUEST);
    ApiError apiError = readJsonAs(response, ApiError.class);
    assertThat(apiError.code()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(apiError.description()).matches("column.*id0.* not found in table.*");
  }

  @Test
  public void columnDeletePartitionKey() {
    final String tableName = testTableName();
    createSimpleTestTable(testKeyspaceName(), tableName);
    String response =
        tryDeleteColumn(testKeyspaceName(), tableName, "id", HttpStatus.SC_BAD_REQUEST);
    ApiError apiError = readJsonAs(response, ApiError.class);
    assertThat(apiError.code()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    // Sub-optimal error message from gRPC/Bridge:
    assertThat(apiError.description())
        // Message varies b/w C* 3.11, 4.0 so need to use very loose pattern
        .matches("Invalid argument.*Cannot drop PRIMARY KEY.*id.*");
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Helper methods
  /////////////////////////////////////////////////////////////////////////
  */

  protected String endpointPathForColumn(String keyspaceName, String tableName, String columnName) {
    return String.format(
        "/v2/schemas/keyspaces/%s/tables/%s/columns/%s", keyspaceName, tableName, columnName);
  }

  protected String endpointPathForAllColumns(String keyspaceName, String tableName) {
    return String.format("/v2/schemas/keyspaces/%s/tables/%s/columns", keyspaceName, tableName);
  }

  protected String tryAddColumn(
      String keyspaceName, String tableName, Sgv2ColumnDefinition columnDef, int expectedResult) {
    return givenWithAuth()
        .contentType(ContentType.JSON)
        .body(asJsonString(columnDef))
        .when()
        .post(endpointPathForAllColumns(keyspaceName, tableName))
        .then()
        .statusCode(expectedResult)
        .extract()
        .asString();
  }

  protected String tryUpdateColumn(
      String keyspaceName,
      String tableName,
      String columnName,
      Sgv2ColumnDefinition columnDef,
      int expectedResult) {
    return givenWithAuth()
        .contentType(ContentType.JSON)
        .body(asJsonString(columnDef))
        .when()
        .put(endpointPathForColumn(keyspaceName, tableName, columnName))
        .then()
        .statusCode(expectedResult)
        .extract()
        .asString();
  }

  protected String tryDeleteColumn(
      String keyspaceName, String tableName, String columnName, int expectedResult) {
    return givenWithAuth()
        .when()
        .delete(endpointPathForColumn(keyspaceName, tableName, columnName))
        .then()
        .statusCode(expectedResult)
        .extract()
        .asString();
  }

  protected String tryFindOneColumn(
      String keyspaceName, String tableName, String columnName, int expectedStatus) {
    return givenWithAuth()
        .queryParam("raw", true)
        .when()
        .get(endpointPathForColumn(keyspaceName, tableName, columnName))
        .then()
        .statusCode(expectedStatus)
        .extract()
        .asString();
  }

  protected Sgv2ColumnDefinition findOneColumn(
      String keyspaceName, String tableName, String columnName, boolean raw) {
    String response =
        givenWithAuth()
            .queryParam("raw", raw)
            .when()
            .get(endpointPathForColumn(keyspaceName, tableName, columnName))
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    if (raw) {
      return readJsonAs(response, Sgv2ColumnDefinition.class);
    }
    return readWrappedRESTResponse(response, Sgv2ColumnDefinition.class);
  }

  protected String tryFindAllColumns(String keyspaceName, String tableName, int expectedStatus) {
    return givenWithAuth()
        .queryParam("raw", true)
        .when()
        .get(endpointPathForAllColumns(keyspaceName, tableName))
        .then()
        .statusCode(expectedStatus)
        .extract()
        .asString();
  }

  protected Sgv2ColumnDefinition[] findAllColumns(
      String keyspaceName, String tableName, boolean raw) {
    String response =
        givenWithAuth()
            .queryParam("raw", raw)
            .when()
            .get(endpointPathForAllColumns(keyspaceName, tableName))
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    if (raw) {
      return readJsonAs(response, Sgv2ColumnDefinition[].class);
    }
    return readWrappedRESTResponse(response, Sgv2ColumnDefinition[].class);
  }

  protected void verifySimpleColumns(Sgv2ColumnDefinition[] foundColumns) {
    assertThat(foundColumns)
        .hasSize(4)
        .containsAll(
            Arrays.asList(
                new Sgv2ColumnDefinition("id", "uuid", false),
                new Sgv2ColumnDefinition("lastName", "text", false),
                new Sgv2ColumnDefinition("firstName", "text", false),
                new Sgv2ColumnDefinition("age", "int", false)));
  }
}
