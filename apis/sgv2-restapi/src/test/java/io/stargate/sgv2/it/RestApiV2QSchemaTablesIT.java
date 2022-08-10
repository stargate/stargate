package io.stargate.sgv2.it;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.common.testprofiles.IntegrationTestProfile;
import io.stargate.sgv2.restapi.service.models.Sgv2ColumnDefinition;
import io.stargate.sgv2.restapi.service.models.Sgv2Table;
import io.stargate.sgv2.restapi.service.models.Sgv2TableAddRequest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.enterprise.context.control.ActivateRequestContext;
import org.apache.http.HttpStatus;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestInstance;

@QuarkusTest
@TestProfile(IntegrationTestProfile.class)
@ActivateRequestContext
@TestClassOrder(ClassOrderer.DisplayName.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RestApiV2QSchemaTablesIT extends RestApiV2QIntegrationTestBase {
  private static final String BASE_PATH = "/v2/schemas/tables/";

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
    Assertions.fail("Test not implemented");
  }

  @Test
  public void tablesGetRaw() {
    Assertions.fail("Test not implemented");
  }

  @Test
  public void tableGetWrapped() {
    Assertions.fail("Test not implemented");
  }

  @Test
  public void tableGetRaw() {
    Assertions.fail("Test not implemented");
  }

  @Test
  public void tableGetComplex() {
    Assertions.fail("Test not implemented");
  }

  @Test
  public void tableGetFailNotFound() {
    Assertions.fail("Test not implemented");
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

    String response =
        given()
            .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
            .queryParam("raw", "true")
            .when()
            .get(endpointPathForTable(testKeyspaceName(), tableName))
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    final Sgv2Table table = readJsonAs(response, Sgv2Table.class);

    assertThat(table.getKeyspace()).isEqualTo(testKeyspaceName());
    assertThat(table.getName()).isEqualTo(tableName);
    assertThat(table.getColumnDefinitions()).isNotNull();
  }

  @Test
  public void tableCreateWithNullOptions() {
    Assertions.fail("Test not implemented");
  }

  @Test
  public void tableCreateWithNoClustering() {
    Assertions.fail("Test not implemented");
  }

  @Test
  public void tableCreateWithMultClustering() {
    Assertions.fail("Test not implemented");
  }

  @Test
  public void tableCreaateWithMixedCaseNames() {
    Assertions.fail("Test not implemented");
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Tests: Update
  /////////////////////////////////////////////////////////////////////////
   */

  @Test
  public void tableUpdateSimple() {
    Assertions.fail("Test not implemented");
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Tests: Delete
  /////////////////////////////////////////////////////////////////////////
   */

  @Test
  public void tableDelete() {
    Assertions.fail("Test not implemented");
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Utility methods
  /////////////////////////////////////////////////////////////////////////
   */

  private String endpointPathForTables(String ksName) {
    return String.format("/v2/schemas/keyspaces/%s/tables", ksName);
  }

  private String endpointPathForTable(String ksName, String tableName) {
    return String.format("/v2/schemas/keyspaces/%s/tables/%s", ksName, tableName);
  }

  private void createSimpleTestTable(String keyspaceName, String tableName) {
    Sgv2TableAddRequest tableAdd = new Sgv2TableAddRequest();
    tableAdd.setName(tableName);

    List<Sgv2ColumnDefinition> columnDefinitions = new ArrayList<>();

    columnDefinitions.add(new Sgv2ColumnDefinition("id", "uuid", false));
    columnDefinitions.add(new Sgv2ColumnDefinition("lastName", "text", false));
    columnDefinitions.add(new Sgv2ColumnDefinition("firstName", "text", false));
    columnDefinitions.add(new Sgv2ColumnDefinition("age", "int", false));

    tableAdd.setColumnDefinitions(columnDefinitions);

    Sgv2Table.PrimaryKey primaryKey = new Sgv2Table.PrimaryKey();
    primaryKey.setPartitionKey(Arrays.asList("id"));
    tableAdd.setPrimaryKey(primaryKey);

    given()
        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
        .contentType(ContentType.JSON)
        .body(asJsonString(tableAdd))
        .when()
        .post(endpointPathForTables(keyspaceName))
        .then()
        .statusCode(HttpStatus.SC_CREATED);
  }
}
