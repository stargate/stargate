package io.stargate.sgv2.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.exception.model.dto.ApiError;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@QuarkusTestResource(StargateTestResource.class)
public class RestApiV2QRowAddIT extends RestApiV2QIntegrationTestBase {
  public RestApiV2QRowAddIT() {
    super("rowadd_ks_", "rowadd_t_", KeyspaceCreation.PER_CLASS);
  }

  @Test
  public void addRow() {
    final String tableName = testTableName();
    createSimpleTestTable(testKeyspaceName(), tableName);

    String rowIdentifier = UUID.randomUUID().toString();
    Map<String, String> row = new LinkedHashMap<>();
    row.put("id", rowIdentifier);
    row.put("firstName", "John");

    String body =
        givenWithAuth()
            .contentType(ContentType.JSON)
            .body(asJsonString(row))
            .when()
            .post(endpointPathForRowAdd(testKeyspaceName(), tableName))
            .then()
            .statusCode(HttpStatus.SC_CREATED)
            .extract()
            .asString();
    Map<?, ?> rowResponse = readJsonAs(body, Map.class);
    assertThat(rowResponse.get("id")).isEqualTo(rowIdentifier);
    assertThat(rowResponse).isEqualTo(row);

    // And verify row was actually added
    List<Map<String, Object>> data = findRowsAsList(testKeyspaceName(), tableName, rowIdentifier);
    assertThat(data).hasSize(1);
    assertThat(data.get(0).get("firstName")).isEqualTo("John");
  }

  // 12-Aug-2022, tatu: Bit odd test, ported over from "addRowWithCounter()"; apparently
  //     must use "UPDATE" (that is, PUT) if "counter"-valued fields exist
  @Test
  public void addRowWithCounterInvalid() {
    final String tableName = testTableName();
    createTestTable(
        testKeyspaceName(),
        tableName,
        Arrays.asList("id text", "counter counter"),
        Arrays.asList("id"),
        null);

    Map<String, String> row = new HashMap<>();
    row.put("id", UUID.randomUUID().toString());
    row.put("counter", "+1");

    String response =
        insertRowExpectStatus(testKeyspaceName(), tableName, row, HttpStatus.SC_BAD_REQUEST);
    ApiError error = readJsonAs(response, ApiError.class);
    assertThat(error.code()).isEqualTo(HttpStatus.SC_BAD_REQUEST);

    // Could try checking the response, but we seem to get gRPC message (possibly
    // room for improvement there, too)
  }

  @Test
  public void addRowWithList() {
    final String tableName = testTableName();
    createTestTable(
        testKeyspaceName(),
        tableName,
        Arrays.asList("name text", "email list<text>"),
        Arrays.asList("name"),
        null);

    Map<String, String> row = new HashMap<>();
    row.put("name", "alice");
    row.put("email", "['foo@example.com','bar@example.com']");
    insertRow(testKeyspaceName(), tableName, row);

    // And verify
    List<Map<String, Object>> data = findRowsAsList(testKeyspaceName(), tableName, "alice");
    assertThat(data).hasSize(1);
    assertThat(data.get(0).get("name")).isEqualTo("alice");
    assertThat(data.get(0).get("email"))
        .isEqualTo(Arrays.asList("foo@example.com", "bar@example.com"));
  }

  @Test
  public void addRowInvalidField() {
    final String tableName = testTableName();
    createSimpleTestTable(testKeyspaceName(), tableName);

    Map<String, String> row = new HashMap<>();
    row.put("id", UUID.randomUUID().toString());
    row.put("invalid_field", "John");

    String response =
        insertRowExpectStatus(testKeyspaceName(), tableName, row, HttpStatus.SC_BAD_REQUEST);
    ApiError error = readJsonAs(response, ApiError.class);
    assertThat(error.code()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(error.description()).contains("Unknown field name 'invalid_field'");
  }

  @Test
  public void addRowInvalidKey() {
    final String tableName = testTableName();
    createSimpleTestTable(testKeyspaceName(), tableName);

    Map<String, String> row = new HashMap<>();
    row.put("id", "not-really-uuid");
    row.put("firstName", "John");

    String response =
        insertRowExpectStatus(testKeyspaceName(), tableName, row, HttpStatus.SC_BAD_REQUEST);
    ApiError error = readJsonAs(response, ApiError.class);
    assertThat(error.code()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(error.description())
        .contains("Invalid path for row to create, problem")
        .contains("Invalid String value")
        .contains("'not-really-uuid'");
  }

  @Test
  public void addRowWithInvalidJson() {
    final String tableName = testTableName();
    createSimpleTestTable(testKeyspaceName(), tableName);

    // Note: missing double-quote after "firstName"
    String rowJSON = "{\"id\": \"af2603d2-8c03-11eb-a03f-0ada685e0000\",\"firstName: \"john\"}";
    String response =
        givenWithAuth()
            .contentType(ContentType.JSON)
            .body(rowJSON)
            .when()
            .post(endpointPathForRowAdd(testKeyspaceName(), tableName))
            .then()
            .statusCode(HttpStatus.SC_BAD_REQUEST)
            .extract()
            .asString();
    ApiError error = readJsonAs(response, ApiError.class);
    assertThat(error.code()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(error.description()).contains("Invalid JSON payload");
  }
}
