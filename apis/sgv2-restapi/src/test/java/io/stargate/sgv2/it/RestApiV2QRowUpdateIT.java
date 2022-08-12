package io.stargate.sgv2.it;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.common.testprofiles.IntegrationTestProfile;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
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
public class RestApiV2QRowUpdateIT extends RestApiV2QIntegrationTestBase {
  public RestApiV2QRowUpdateIT() {
    super("rowupd_ks_", "rowupd_t_");
  }

  @Test
  public void updateRow() {
    final String tableName = testTableName();
    createSimpleTestTable(testKeyspaceName(), tableName);

    final String rowIdentifier = UUID.randomUUID().toString();
    Map<String, String> row = new HashMap<>();
    row.put("id", rowIdentifier);
    row.put("firstName", "John");
    insertRow(testKeyspaceName(), tableName, row);

    Map<String, String> rowUpdate = new HashMap<>();
    rowUpdate.put("firstName", "Robert");
    rowUpdate.put("lastName", "Plant");
    String updateResponse =
        updateRowReturnResponse(
            endpointPathForRowByPK(testKeyspaceName(), tableName, rowIdentifier), false, rowUpdate);
    Map<String, String> data = readWrappedRESTResponse(updateResponse, Map.class);
    assertThat(data).containsAllEntriesOf(rowUpdate);
  }

  @Test
  public void updateRowRaw() {
    final String tableName = testTableName();
    createSimpleTestTable(testKeyspaceName(), tableName);

    final String rowIdentifier = UUID.randomUUID().toString();
    Map<String, String> row = new HashMap<>();
    row.put("id", rowIdentifier);
    row.put("firstName", "John");
    row.put("lastName", "Lennon");
    insertRow(testKeyspaceName(), tableName, row);

    Map<String, Object> rowUpdate = new HashMap<>();
    rowUpdate.put("firstName", "Jimmy");
    rowUpdate.put("lastName", "Page");
    String updateResponse =
        updateRowReturnResponse(
            endpointPathForRowByPK(testKeyspaceName(), tableName, rowIdentifier), true, rowUpdate);
    Map<String, Object> data = (Map<String, Object>) readJsonAs(updateResponse, Map.class);
    assertThat(data).containsAllEntriesOf(rowUpdate);

    // Additional processing, checking (compared to "updateRow"):
    // First, verify that we can "delete" lastName
    Map<String, String> update2 = new HashMap<>();
    update2.put("firstName", "Roger");
    update2.put("lastName", null);
    updateRowReturnResponse(
        endpointPathForRowByPK(testKeyspaceName(), tableName, rowIdentifier), true, update2);

    // And that change actually occurs
    JsonNode json = findRowsAsJsonNode(testKeyspaceName(), tableName, rowIdentifier);
    assertThat(json.size()).isEqualTo(1);
    assertThat(json.at("/0/id").asText()).isEqualTo(rowIdentifier);
    assertThat(json.at("/0/firstName").asText()).isEqualTo("Roger");
    assertTrue(json.at("/0/lastName").isNull());
    assertTrue(json.at("/0/age").isNull());
    assertThat(json.at("/0").size()).isEqualTo(4);
  }

  private String updateRowReturnResponse(String updatePath, boolean raw, Map<?, ?> payload) {
    return given()
        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
        .queryParam("raw", raw)
        .contentType(ContentType.JSON)
        .body(asJsonString(payload))
        .when()
        .put(updatePath)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .extract()
        .asString();
  }

  @Test
  public void updateRowWithCounter() {
    final String tableName = testTableName();
  }

  @Test
  public void updateRowWithMultipleCounters() {
    final String tableName = testTableName();
  }

  @Test
  public void updateRowWithInvalidJson() {
    final String tableName = testTableName();
  }
}
