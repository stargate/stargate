package io.stargate.sgv2.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.api.common.exception.model.dto.ApiError;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import io.stargate.sgv2.restapi.service.models.Sgv2ColumnDefinition;
import io.stargate.sgv2.restapi.service.models.Sgv2Table;
import io.stargate.sgv2.restapi.service.models.Sgv2TableAddRequest;
import java.util.*;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@QuarkusTestResource(StargateTestResource.class)
public class RestApiV2QRowUpdateIT extends RestApiV2QIntegrationTestBase {
  public RestApiV2QRowUpdateIT() {
    super("rowupd_ks_", "rowupd_t_", KeyspaceCreation.PER_CLASS);
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

  @Test
  public void updateRowWithCounter() {
    final String tableName = testTableName();
    List<Sgv2ColumnDefinition> columnDefs =
        Arrays.asList(
            new Sgv2ColumnDefinition("id", "text", false),
            new Sgv2ColumnDefinition("counter", "counter", false));
    Sgv2Table.PrimaryKey primaryKey = new Sgv2Table.PrimaryKey(Arrays.asList("id"), null);
    final Sgv2TableAddRequest tableAdd =
        new Sgv2TableAddRequest(tableName, primaryKey, columnDefs, false, null);
    createTable(testKeyspaceName(), tableAdd);

    // Since create row cannot increase counter we have to use PUT
    final String rowIdentifier = UUID.randomUUID().toString();
    Map<String, String> rowForPlus1 = Collections.singletonMap("counter", "+1");
    String updateResponse =
        updateRowReturnResponse(
            endpointPathForRowByPK(testKeyspaceName(), tableName, rowIdentifier),
            true,
            rowForPlus1);
    Map<String, Object> responseMap = (Map<String, Object>) readJsonAs(updateResponse, Map.class);
    assertThat(responseMap).containsAllEntriesOf(rowForPlus1).hasSize(1);

    // But let's also explicitly find the row verify counter was increased
    List<Map<String, Object>> rows = findRowsAsList(testKeyspaceName(), tableName, rowIdentifier);
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).get("id")).isEqualTo(rowIdentifier);
    assertThat(rows.get(0).get("counter")).isEqualTo("1");

    // And increase again
    updateRowReturnResponse(
        endpointPathForRowByPK(testKeyspaceName(), tableName, rowIdentifier), true, rowForPlus1);
    // and verify further increase
    rows = findRowsAsList(testKeyspaceName(), tableName, rowIdentifier);
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).get("id")).isEqualTo(rowIdentifier);
    assertThat(rows.get(0).get("counter")).isEqualTo("2");
  }

  @Test
  public void updateRowWithMultipleCounters() {
    final String tableName = testTableName();
    List<Sgv2ColumnDefinition> columnDefs =
        Arrays.asList(
            new Sgv2ColumnDefinition("id", "text", false),
            new Sgv2ColumnDefinition("counter1", "counter", false),
            new Sgv2ColumnDefinition("counter2", "counter", false));
    Sgv2Table.PrimaryKey primaryKey = new Sgv2Table.PrimaryKey(Arrays.asList("id"), null);
    final Sgv2TableAddRequest tableAdd =
        new Sgv2TableAddRequest(tableName, primaryKey, columnDefs, false, null);
    createTable(testKeyspaceName(), tableAdd);

    final String rowIdentifier = UUID.randomUUID().toString();
    Map<String, String> rowUpdate = new HashMap<>();
    rowUpdate.put("counter1", "+1");
    rowUpdate.put("counter2", "-1");

    String updateResponse =
        updateRowReturnResponse(
            endpointPathForRowByPK(testKeyspaceName(), tableName, rowIdentifier), true, rowUpdate);
    Map<String, Object> responseMap = (Map<String, Object>) readJsonAs(updateResponse, Map.class);
    assertThat(responseMap).containsAllEntriesOf(rowUpdate).hasSize(2);

    List<Map<String, Object>> rows = findRowsAsList(testKeyspaceName(), tableName, rowIdentifier);
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).get("id")).isEqualTo(rowIdentifier);
    assertThat(rows.get(0).get("counter1")).isEqualTo("1");
    assertThat(rows.get(0).get("counter2")).isEqualTo("-1");
  }

  @Test
  public void updateRowWithInvalidJson() {
    final String tableName = testTableName();
    createSimpleTestTable(testKeyspaceName(), tableName);

    final String rowIdentifier = UUID.randomUUID().toString();
    Map<String, String> row = new HashMap<>();
    row.put("id", rowIdentifier);
    row.put("firstName", "Graham");
    insertRow(testKeyspaceName(), tableName, row);

    // Invalid JSON, missing double-quote after Robert
    final String invalidJSON = "{\"firstname\": \"Robert,\"lastname\": \"Plant\"}";

    String response =
        updateRowReturnResponse(
            endpointPathForRowByPK(testKeyspaceName(), tableName, rowIdentifier),
            true,
            invalidJSON,
            HttpStatus.SC_BAD_REQUEST);
    ApiError error = readJsonAs(response, ApiError.class);
    assertThat(error.code()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(error.description()).contains("Invalid JSON payload");
  }

  @Test
  public void updateRowWithInvalidKey() {
    final String tableName = testTableName();
    createSimpleTestTable(testKeyspaceName(), tableName);

    // No need to actually create a row is there? Will fail regardless
    Map<String, String> update = Collections.singletonMap("firstName", "Boberta");

    String response =
        updateRowReturnResponse(
            endpointPathForRowByPK(testKeyspaceName(), tableName, "not-a-valid-uuid"),
            true,
            update,
            HttpStatus.SC_BAD_REQUEST);
    ApiError error = readJsonAs(response, ApiError.class);
    assertThat(error.code()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(error.description())
        .contains("Invalid path for row to update")
        .contains("'not-a-valid-uuid'");
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Helper methods
  /////////////////////////////////////////////////////////////////////////
   */
}
