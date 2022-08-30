package io.stargate.sgv2.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.ResourceArg;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.exception.model.dto.ApiError;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestInstance;

@QuarkusIntegrationTest
@QuarkusTestResource(
    value = StargateTestResource.class,
    initArgs =
        @ResourceArg(name = StargateTestResource.Options.DISABLE_FIXED_TOKEN, value = "true"))
@TestClassOrder(ClassOrderer.DisplayName.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RestApiV2QRowPatchIT extends RestApiV2QIntegrationTestBase {
  public RestApiV2QRowPatchIT() {
    super("rowptc_ks_", "rowptc_t_");
  }

  @Test
  public void patchRow() {
    final String tableName = testTableName();
    createSimpleTestTable(testKeyspaceName(), tableName);

    String rowIdentifier = UUID.randomUUID().toString();
    Map<String, String> row = new HashMap<>();
    row.put("id", rowIdentifier);
    row.put("firstName", "John");
    row.put("lastName", "Doe");
    insertRow(testKeyspaceName(), tableName, row);

    Map<String, String> rowUpdate = new HashMap<>();
    rowUpdate.put("firstName", "Jane");

    String patchResponse =
        patchRowReturnResponse(
            endpointPathForRowByPK(testKeyspaceName(), tableName, rowIdentifier), false, rowUpdate);
    Map<String, String> data = readWrappedRESTResponse(patchResponse, Map.class);
    assertThat(data).containsAllEntriesOf(rowUpdate);

    List<Map<String, Object>> rows = findRowsAsList(testKeyspaceName(), tableName, rowIdentifier);
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).get("id")).isEqualTo(rowIdentifier);
    assertThat(rows.get(0).get("firstName")).isEqualTo("Jane");
    assertThat(rows.get(0).get("lastName")).isEqualTo("Doe");
  }

  @Test
  public void patchRowRaw() {
    final String tableName = testTableName();
    createSimpleTestTable(testKeyspaceName(), tableName);

    final String rowIdentifier = UUID.randomUUID().toString();
    Map<String, String> row = new HashMap<>();
    row.put("id", rowIdentifier);
    row.put("firstName", "Jim");
    row.put("lastName", "Doe");
    insertRow(testKeyspaceName(), tableName, row);

    Map<String, String> rowUpdate = new HashMap<>();
    rowUpdate.put("firstName", "Mary");

    String patchResponse =
        patchRowReturnResponse(
            endpointPathForRowByPK(testKeyspaceName(), tableName, rowIdentifier), true, rowUpdate);
    Map<String, String> data = readJsonAs(patchResponse, Map.class);
    assertThat(data).containsAllEntriesOf(rowUpdate);

    List<Map<String, Object>> rows = findRowsAsList(testKeyspaceName(), tableName, rowIdentifier);
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).get("id")).isEqualTo(rowIdentifier);
    assertThat(rows.get(0).get("firstName")).isEqualTo("Mary");
    assertThat(rows.get(0).get("lastName")).isEqualTo("Doe");
  }

  @Test
  public void patchRowInvalidJson() {
    final String tableName = testTableName();
    createSimpleTestTable(testKeyspaceName(), tableName);

    final String rowIdentifier = UUID.randomUUID().toString();
    // No need to actually create an entry; should fail regardless
    final String invalidJSON = "{ missing-quotes: 25. }";

    String response =
        patchRowReturnResponse(
            endpointPathForRowByPK(testKeyspaceName(), tableName, rowIdentifier),
            true,
            invalidJSON,
            HttpStatus.SC_BAD_REQUEST);
    ApiError error = readJsonAs(response, ApiError.class);
    assertThat(error.code()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(error.description()).contains("Invalid JSON payload");
  }

  @Test
  public void patchRowInvalidKey() {
    final String tableName = testTableName();
    createSimpleTestTable(testKeyspaceName(), tableName);

    Map<String, String> rowUpdate = new HashMap<>();
    rowUpdate.put("firstName", "Mary");

    String response =
        patchRowReturnResponse(
            endpointPathForRowByPK(testKeyspaceName(), tableName, "not-an-actual-uuid"),
            true,
            rowUpdate,
            HttpStatus.SC_BAD_REQUEST);
    ApiError error = readJsonAs(response, ApiError.class);
    assertThat(error.code()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(error.description())
        .contains("Invalid path for row to update")
        .contains("'not-an-actual-uuid'");
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Helper methods
  /////////////////////////////////////////////////////////////////////////
   */

  private String patchRowReturnResponse(String patchPath, boolean raw, Map<?, ?> payload) {
    return patchRowReturnResponse(patchPath, raw, payload, HttpStatus.SC_OK);
  }

  private String patchRowReturnResponse(
      String patchPath, boolean raw, Map<?, ?> payloadMap, int expectedStatus) {
    return patchRowReturnResponse(patchPath, raw, asJsonString(payloadMap), expectedStatus);
  }

  private String patchRowReturnResponse(
      String patchPath, boolean raw, String payloadJSON, int expectedStatus) {
    return givenWithAuth()
        .queryParam("raw", raw)
        .contentType(ContentType.JSON)
        .body(payloadJSON)
        .when()
        .patch(patchPath)
        .then()
        .statusCode(expectedStatus)
        .extract()
        .asString();
  }
}
