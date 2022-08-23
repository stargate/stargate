package io.stargate.sgv2.it;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.common.testprofiles.IntegrationTestProfile;
import io.stargate.sgv2.restapi.service.models.Sgv2ColumnDefinition;
import javax.enterprise.context.control.ActivateRequestContext;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestInstance;

@QuarkusTest
@TestProfile(IntegrationTestProfile.class)
@ActivateRequestContext
@TestClassOrder(ClassOrderer.DisplayName.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
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
  public void columnAddBadType() {}

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
  public void columnAddStatic() {}

  /*
  /////////////////////////////////////////////////////////////////////////
  // Test methods, Get single
  /////////////////////////////////////////////////////////////////////////
   */

  @Test
  public void columnGetBadKeyspace() {}

  @Test
  public void columnGetBadTable() {}

  @Test
  public void columnGetComplex() {}

  @Test
  public void columnGetNotFound() {}

  @Test
  public void columnGetRaw() {}

  @Test
  public void columnGetWrapped() {}

  /*
  /////////////////////////////////////////////////////////////////////////
  // Test methods, Get multiple
  /////////////////////////////////////////////////////////////////////////
   */

  @Test
  public void columnsGetBadKeyspace() {}

  @Test
  public void columnsGetBadTable() {}

  @Test
  public void columnsGetComplex() {}

  @Test
  public void columnsGetRaw() {}

  @Test
  public void columnsGetWrapped() {}

  /*
  /////////////////////////////////////////////////////////////////////////
  // Test methods, Update
  /////////////////////////////////////////////////////////////////////////
   */

  @Test
  public void columnUpdate() {}

  @Test
  public void columnUpdateBadKeyspace() {}

  @Test
  public void columnUpdateBadTable() {}

  @Test
  public void columnUpdateNotFound() {}

  /*
  /////////////////////////////////////////////////////////////////////////
  // Test methods, Delete
  /////////////////////////////////////////////////////////////////////////
   */

  @Test
  public void columnDelete() {}

  @Test
  public void columnDeleteBadKeyspace() {}

  @Test
  public void columnDeleteBadTable() {}

  @Test
  public void columnDeleteNotFound() {}

  @Test
  public void columnDeletePartitionKey() {}

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
    return given()
        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
        .contentType(ContentType.JSON)
        .body(asJsonString(columnDef))
        .when()
        .post(endpointPathForAllColumns(keyspaceName, tableName))
        .then()
        .statusCode(expectedResult)
        .extract()
        .asString();
  }

  protected Sgv2ColumnDefinition findOneColumn(
      String keyspaceName, String tableName, String columnName, boolean raw) {
    String response =
        given()
            .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
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
}
