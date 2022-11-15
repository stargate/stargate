package io.stargate.sgv2.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.exception.model.dto.ApiError;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import io.stargate.sgv2.restapi.service.models.Sgv2ColumnDefinition;
import io.stargate.sgv2.restapi.service.models.Sgv2Table;
import io.stargate.sgv2.restapi.service.models.Sgv2TableAddRequest;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@QuarkusTestResource(StargateTestResource.class)
public class RestApiV2QSchemaTablesIT extends RestApiV2QIntegrationTestBase {
  public RestApiV2QSchemaTablesIT() {
    super("tbl_ks_", "tbl_t_");
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Tests: GET
  /////////////////////////////////////////////////////////////////////////
   */

  @Test
  public void tablesGetWrapped() {
    String body =
        givenWithAuth()
            .when()
            .get(endpointPathForTables("system"))
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    assertSystemTables(readWrappedRESTResponse(body, Sgv2Table[].class));
  }

  @Test
  public void tablesGetRaw() {
    String body =
        givenWithAuth()
            .queryParam("raw", "true")
            .when()
            .get(endpointPathForTables("system"))
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    assertSystemTables(readJsonAs(body, Sgv2Table[].class));
  }

  private void assertSystemTables(Sgv2Table[] systemTables) {
    assertThat(systemTables.length).isGreaterThan(5);
    assertThat(systemTables)
        .anySatisfy(
            value ->
                assertThat(value)
                    .usingRecursiveComparison()
                    .ignoringFields("columnDefinitions", "primaryKey", "tableOptions")
                    .isEqualTo(new Sgv2Table("local", "system", null, null, null)));
  }

  @Test
  public void tableGetWrapped() {
    String body =
        givenWithAuth()
            .when()
            .get(endpointPathForTable("system", "local"))
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    assertSystemTable(readWrappedRESTResponse(body, Sgv2Table.class), "local");
  }

  @Test
  public void tableGetRaw() {
    String body =
        givenWithAuth()
            .queryParam("raw", "true")
            .when()
            .get(endpointPathForTable("system", "local"))
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    assertSystemTable(readJsonAs(body, Sgv2Table.class), "local");
  }

  private void assertSystemTable(Sgv2Table table, String expName) {
    assertThat(table.getKeyspace()).isEqualTo("system");
    assertThat(table.getName()).isEqualTo(expName);
    assertThat(table.getColumnDefinitions()).isNotNull().isNotEmpty();
  }

  @Test
  public void tableGetComplex() {
    final String tableName = testTableName();
    NameResponse createResponse = createComplexTestTable(testKeyspaceName(), tableName);
    assertThat(createResponse.name).isEqualTo(tableName);

    Sgv2Table table = findTable(testKeyspaceName(), tableName);
    assertThat(table.getKeyspace()).isEqualTo(testKeyspaceName());
    assertThat(table.getName()).isEqualTo(tableName);
    assertThat(table.getColumnDefinitions())
        .hasSize(4)
        .hasSameElementsAs(
            Arrays.asList(
                new Sgv2ColumnDefinition("pk0", "uuid", false),
                new Sgv2ColumnDefinition("col1", "frozen<map<date, text>>", false),
                new Sgv2ColumnDefinition("col2", "frozen<set<boolean>>", false),
                new Sgv2ColumnDefinition("col3", "tuple<duration, inet>", false)));
  }

  @Test
  public void tableGetFailNotFound() {
    assertTableNotFound(testKeyspaceName(), "no-such-table");
  }

  private void assertTableNotFound(String keyspaceName, String tableName) {
    String body =
        givenWithAuth()
            .when()
            .get(endpointPathForTable(keyspaceName, tableName))
            .then()
            .statusCode(HttpStatus.SC_NOT_FOUND)
            .extract()
            .asString();
    ApiError error = readJsonAs(body, ApiError.class);
    assertThat(error.code()).isEqualTo(HttpStatus.SC_NOT_FOUND);
    assertThat(error.description()).matches("Table.*" + tableName + ".*not found.*");
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Tests: Create
  /////////////////////////////////////////////////////////////////////////
   */

  @Test
  public void tableCreateBasic() {
    final String tableName = testTableName();
    createSimpleTestTable(testKeyspaceName(), tableName);

    final Sgv2Table table = findTable(testKeyspaceName(), tableName);

    assertThat(table.getKeyspace()).isEqualTo(testKeyspaceName());
    assertThat(table.getName()).isEqualTo(tableName);
    assertThat(table.getColumnDefinitions()).isNotNull();
  }

  @Test
  public void tableCreateWithNestedMap() {
    final String tableName = testTableName();
    createTestTable(
        testKeyspaceName(),
        tableName,
        Arrays.asList("id bigint", "data map<text,frozen<list<frozen<tuple<double,double>>>>>"),
        Arrays.asList("id"),
        Arrays.asList());

    final Sgv2Table table = findTable(testKeyspaceName(), tableName);

    assertThat(table.getKeyspace()).isEqualTo(testKeyspaceName());
    assertThat(table.getName()).isEqualTo(tableName);
  }

  @Test
  public void tableCreateWithNullOptions() {
    final String tableName = "t1"; // not sure why but was that way in original test
    final Sgv2TableAddRequest tableAdd = new Sgv2TableAddRequest(tableName);
    final List<Sgv2ColumnDefinition> columns =
        Arrays.asList(
            new Sgv2ColumnDefinition("id", "uuid", false),
            new Sgv2ColumnDefinition("lastName", "text", false),
            new Sgv2ColumnDefinition("firstName", "text", false));
    tableAdd.setColumnDefinitions(columns);
    Sgv2Table.PrimaryKey primaryKey = new Sgv2Table.PrimaryKey();
    primaryKey.setPartitionKey(Arrays.asList("id"));
    tableAdd.setPrimaryKey(primaryKey);
    tableAdd.setTableOptions(null);

    // First verify response
    NameResponse response = createTable(testKeyspaceName(), tableAdd);
    assertThat(response.name).isEqualTo(tableName);

    // And then find the table itself
    final Sgv2Table table = findTable(testKeyspaceName(), tableName);
    assertThat(table.getName()).isEqualTo(tableName);
    // Alas, returned column order unpredictable, so:
    assertThat(table.getColumnDefinitions()).hasSameElementsAs(columns);
  }

  @Test
  public void tableCreateWithNoClustering() {
    final String tableName = testTableName();
    final Sgv2TableAddRequest tableAdd = new Sgv2TableAddRequest(tableName);

    final List<Sgv2ColumnDefinition> columns =
        Arrays.asList(
            new Sgv2ColumnDefinition("pk1", "int", false),
            new Sgv2ColumnDefinition("ck1", "int", false));
    tableAdd.setColumnDefinitions(columns);
    Sgv2Table.PrimaryKey primaryKey = new Sgv2Table.PrimaryKey();
    primaryKey.setPartitionKey(Arrays.asList("pk1"));
    primaryKey.setClusteringKey(Arrays.asList("ck1"));
    tableAdd.setPrimaryKey(primaryKey);
    tableAdd.setTableOptions(new Sgv2Table.TableOptions(0, null));

    // First verify response
    NameResponse response = createTable(testKeyspaceName(), tableAdd);
    assertThat(response.name).isEqualTo(tableAdd.getName());

    // And then find the table itself
    final Sgv2Table table = findTable(testKeyspaceName(), tableName);
    assertThat(table.getName()).isEqualTo(tableAdd.getName());
    assertThat(table.getColumnDefinitions()).hasSameElementsAs(columns);
    assertThat(table.getTableOptions().getClusteringExpression().get(0).getOrder())
        .isEqualTo("ASC");
  }

  @Test
  public void tableCreateWithMultClustering() {
    final String tableName = testTableName();
    final Sgv2TableAddRequest tableAdd = new Sgv2TableAddRequest(tableName);
    final List<Sgv2ColumnDefinition> columns =
        Arrays.asList(
            new Sgv2ColumnDefinition("value", "int", false),
            new Sgv2ColumnDefinition("ck2", "int", false),
            new Sgv2ColumnDefinition("ck1", "int", false),
            new Sgv2ColumnDefinition("pk2", "int", false),
            new Sgv2ColumnDefinition("pk1", "int", false));
    tableAdd.setColumnDefinitions(columns);
    // Create partition and clustering keys in order different from that of all-columns
    // definitions
    Sgv2Table.PrimaryKey primaryKey = new Sgv2Table.PrimaryKey();
    primaryKey.setPartitionKey(Arrays.asList("pk1", "pk2"));
    primaryKey.setClusteringKey(Arrays.asList("ck1", "ck2"));
    tableAdd.setPrimaryKey(primaryKey);
    tableAdd.setTableOptions(new Sgv2Table.TableOptions(0, null));
    createTable(testKeyspaceName(), tableAdd);

    final Sgv2Table table = findTable(testKeyspaceName(), tableName);
    // First, verify that partition key ordering is like we expect
    assertThat(table.getTableOptions().getClusteringExpression().size()).isEqualTo(2);
    assertThat(table.getTableOptions().getClusteringExpression().get(0).getColumn())
        .isEqualTo("ck1");
    assertThat(table.getTableOptions().getClusteringExpression().get(1).getColumn())
        .isEqualTo("ck2");

    // And then the same wrt full primary key definition
    Sgv2Table.PrimaryKey pk = table.getPrimaryKey();
    assertThat(pk.getPartitionKey().size()).isEqualTo(2);
    assertThat(pk.getPartitionKey()).isEqualTo(Arrays.asList("pk1", "pk2"));
    assertThat(pk.getClusteringKey().size()).isEqualTo(2);
    assertThat(pk.getClusteringKey()).isEqualTo(Arrays.asList("ck1", "ck2"));
  }

  @Test
  public void tableCreateWithMixedCaseNames() {
    final String tableName = testTableName();
    final Sgv2TableAddRequest tableAdd = new Sgv2TableAddRequest(tableName);

    final List<Sgv2ColumnDefinition> columns =
        Arrays.asList(
            new Sgv2ColumnDefinition("ID", "uuid", false),
            new Sgv2ColumnDefinition("Lastname", "text", false),
            new Sgv2ColumnDefinition("Firstname", "text", false));
    tableAdd.setColumnDefinitions(columns);
    Sgv2Table.PrimaryKey primaryKey = new Sgv2Table.PrimaryKey();
    primaryKey.setPartitionKey(Arrays.asList("ID"));
    tableAdd.setPrimaryKey(primaryKey);

    NameResponse response = createTable(testKeyspaceName(), tableAdd);
    assertThat(response.name).isEqualTo(tableName);

    // Then insert row: should use convenience methods in future but for now done inline
    String rowIdentifier = UUID.randomUUID().toString();
    Map<String, String> row = new HashMap<>();
    row.put("ID", rowIdentifier);
    row.put("Firstname", "John");
    row.put("Lastname", "Doe");
    insertRow(testKeyspaceName(), tableName, row);

    String whereClause = String.format("{\"ID\":{\"$eq\":\"%s\"}}", rowIdentifier);
    String body =
        givenWithAuth()
            .queryParam("where", whereClause)
            .contentType(ContentType.JSON)
            .when()
            .get(endpointPathForRowGetWith(testKeyspaceName(), tableName))
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    ListOfMapsGetResponseWrapper wrapper = readJsonAs(body, ListOfMapsGetResponseWrapper.class);
    List<Map<String, Object>> data = wrapper.getData();
    assertThat(data).hasSize(1);
    assertThat(data.get(0).get("ID")).isEqualTo(rowIdentifier);
    assertThat(data.get(0).get("Firstname")).isEqualTo("John");
    assertThat(data.get(0).get("Lastname")).isEqualTo("Doe");
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Tests: Update
  /////////////////////////////////////////////////////////////////////////
   */

  @Test
  public void tableUpdateSimple() {
    final String tableName = testTableName();
    createSimpleTestTable(testKeyspaceName(), tableName);

    Sgv2TableAddRequest tableUpdate = new Sgv2TableAddRequest(tableName);
    Sgv2Table.TableOptions tableOptions = new Sgv2Table.TableOptions();
    tableOptions.setDefaultTimeToLive(5);
    tableUpdate.setTableOptions(tableOptions);

    String response =
        givenWithAuth()
            .contentType(ContentType.JSON)
            .body(asJsonString(tableUpdate))
            .when()
            .put(endpointPathForTable(testKeyspaceName(), tableName))
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    NameResponse putResponse = readJsonAs(response, NameResponse.class);
    assertThat(putResponse.name).isEqualTo(tableName);

    final Sgv2Table modifiedTable = findTable(testKeyspaceName(), tableName);
    assertThat(modifiedTable.getKeyspace()).isEqualTo(testKeyspaceName());
    assertThat(modifiedTable.getName()).isEqualTo(tableName);
    assertThat(modifiedTable.getColumnDefinitions()).isNotNull().isNotEmpty();

    assertThat(modifiedTable.getTableOptions()).isNotNull();
    assertThat(modifiedTable.getTableOptions().getDefaultTimeToLive()).isEqualTo(5);
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Tests: Delete
  /////////////////////////////////////////////////////////////////////////
   */

  @Test
  public void tableDelete() {
    final String tableName = testTableName();
    createSimpleTestTable(testKeyspaceName(), tableName);

    final Sgv2Table table = findTable(testKeyspaceName(), tableName);
    assertThat(table.getName()).isEqualTo(tableName);

    givenWithAuth()
        .when()
        .delete(endpointPathForTable(testKeyspaceName(), tableName))
        .then()
        .statusCode(HttpStatus.SC_NO_CONTENT);

    // And then confirm it is gone
    assertTableNotFound(testKeyspaceName(), tableName);
  }
}
