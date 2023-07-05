package io.stargate.sgv2.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class RestApiV2QMapTestsImplIT {
  public static void addRowWithCompactMap(
      RestApiV2QIntegrationTestBase testBase, boolean serverFlag, boolean testDefault) {
    Boolean optimizeMapData = getFlagForCompactDataTest(serverFlag, testDefault);
    final String tableName = testBase.testTableName() + (testDefault ? "1" : "2");
    testBase.createTestTable(
        testBase.testKeyspaceName(),
        tableName,
        Arrays.asList("name text", "properties map<text,text>", "events map<int,boolean>"),
        Arrays.asList("name"),
        null);

    Map<String, String> row = new HashMap<>();
    row.put("name", "alice");
    row.put("properties", "{'key1': 'value1', 'key2': 'value2'}");
    row.put("events", "{123: true, 456: false}");
    testBase.insertRow(testBase.testKeyspaceName(), tableName, row, optimizeMapData);

    // And verify
    JsonNode readRow =
        testBase.findRowsAsJsonNode(
            testBase.testKeyspaceName(), tableName, optimizeMapData, "alice");
    assertThat(readRow).hasSize(1);
    assertThat(readRow.get(0).get("name").asText()).isEqualTo("alice");
    assertThat(readRow.get(0).get("properties").get("key1").asText()).isEqualTo("value1");
    assertThat(readRow.get(0).get("properties").get("key2").asText()).isEqualTo("value2");
    assertThat(readRow.get(0).get("events").get("123").asBoolean()).isEqualTo(true);
    assertThat(readRow.get(0).get("events").get("456").asBoolean()).isEqualTo(false);
  }

  public static void addRowWithNonCompactMap(
      RestApiV2QIntegrationTestBase testBase, boolean serverFlag, boolean testDefault) {
    Boolean optimizeMapData = getFlagForNonCompactDataTest(serverFlag, testDefault);
    final String tableName = testBase.testTableName() + (testDefault ? "1" : "2");
    testBase.createTestTable(
        testBase.testKeyspaceName(),
        tableName,
        Arrays.asList("name text", "properties map<text,text>", "events map<int,boolean>"),
        Arrays.asList("name"),
        null);

    Map<String, String> row = new HashMap<>();
    row.put("name", "alice");
    row.put(
        "properties", "[{'key': 'key1', 'value': 'value1' }, {'key': 'key2', 'value' : 'value2'}]");
    row.put("events", "[{'key': 123, 'value': true }, {'key': 456, 'value' : false}]");
    testBase.insertRow(testBase.testKeyspaceName(), tableName, row, optimizeMapData);

    // And verify
    JsonNode readRow =
        testBase.findRowsAsJsonNode(
            testBase.testKeyspaceName(), tableName, optimizeMapData, "alice");
    assertThat(readRow).hasSize(1);
    assertThat(readRow.get(0).get("name").asText()).isEqualTo("alice");
    assertThat(readRow.get(0).get("properties").get(0).get("key").asText()).isEqualTo("key1");
    assertThat(readRow.get(0).get("properties").get(0).get("value").asText()).isEqualTo("value1");
    assertThat(readRow.get(0).get("properties").get(1).get("key").asText()).isEqualTo("key2");
    assertThat(readRow.get(0).get("properties").get(1).get("value").asText()).isEqualTo("value2");
    assertThat(readRow.get(0).get("events").get(0).get("key").asInt()).isEqualTo(123);
    assertThat(readRow.get(0).get("events").get(0).get("value").asBoolean()).isEqualTo(true);
    assertThat(readRow.get(0).get("events").get(1).get("key").asInt()).isEqualTo(456);
    assertThat(readRow.get(0).get("events").get(1).get("value").asBoolean()).isEqualTo(false);
  }

  public static void updateRowWithCompactMap(
      RestApiV2QIntegrationTestBase testBase, boolean serverFlag, boolean testDefault) {
    Boolean optimizeMapData = getFlagForCompactDataTest(serverFlag, testDefault);
    final String tableName = testBase.testTableName() + (testDefault ? "1" : "2");
    testBase.createTestTable(
        testBase.testKeyspaceName(),
        tableName,
        Arrays.asList(
            "id uuid", "name text", "properties map<text,text>", "events map<int,boolean>"),
        Arrays.asList("id"),
        null);

    final String rowIdentifier = UUID.randomUUID().toString();
    Map<String, String> row = new HashMap<>();
    row.put("id", rowIdentifier);
    row.put("name", "John");
    row.put("properties", "{'key1': 'value1', 'key2': 'value2'}");
    row.put("events", "{123: true, 456: false}");
    testBase.insertRow(testBase.testKeyspaceName(), tableName, row, optimizeMapData);

    Map<String, Object> rowUpdate = new HashMap<>();
    rowUpdate.put("name", "Jimmy");
    rowUpdate.put("properties", "{'key1': 'value11', 'key2': 'value12'}");
    rowUpdate.put("events", "{123: false}");
    String updateResponse =
        testBase.updateRowReturnResponse(
            testBase.endpointPathForRowByPK(testBase.testKeyspaceName(), tableName, rowIdentifier),
            true,
            rowUpdate,
            optimizeMapData);
    Map<String, Object> data = (Map<String, Object>) testBase.readJsonAs(updateResponse, Map.class);
    assertThat(data).containsAllEntriesOf(rowUpdate);

    // Additional processing, checking (compared to "updateRow"):
    Map<String, String> update2 = new HashMap<>();
    update2.put("properties", "{'key1': 'value11'}");
    update2.put("events", null);
    testBase.updateRowReturnResponse(
        testBase.endpointPathForRowByPK(testBase.testKeyspaceName(), tableName, rowIdentifier),
        true,
        update2,
        optimizeMapData);

    // And that change actually occurs
    JsonNode json =
        testBase.findRowsAsJsonNode(
            testBase.testKeyspaceName(), tableName, optimizeMapData, rowIdentifier);
    assertThat(json.size()).isEqualTo(1);
    assertThat(json.get(0).get("id").asText()).isEqualTo(rowIdentifier);
    assertThat(json.get(0).get("name").asText()).isEqualTo("Jimmy");
    assertThat(json.get(0).get("properties").get("key1").asText()).isEqualTo("value11");
    assertTrue(json.get(0).get("properties").at("/key2").isMissingNode());
    assertTrue(json.get(0).at("/events").isEmpty());
  }

  public static void updateRowWithNonCompactMap(
      RestApiV2QIntegrationTestBase testBase, boolean serverFlag, boolean testDefault) {
    Boolean optimizeMapData = getFlagForNonCompactDataTest(serverFlag, testDefault);
    final String tableName = testBase.testTableName() + (testDefault ? "1" : "2");
    testBase.createTestTable(
        testBase.testKeyspaceName(),
        tableName,
        Arrays.asList(
            "id uuid", "name text", "properties map<text,text>", "events map<int,boolean>"),
        Arrays.asList("id"),
        null);

    final String rowIdentifier = UUID.randomUUID().toString();
    Map<String, String> row = new HashMap<>();
    row.put("id", rowIdentifier);
    row.put("name", "John");
    row.put(
        "properties", "[{'key': 'key1', 'value': 'value1' }, {'key': 'key2', 'value' : 'value2'}]");
    row.put("events", "[{'key': 123, 'value': true }, {'key': 456, 'value' : false}]");
    testBase.insertRow(testBase.testKeyspaceName(), tableName, row, optimizeMapData);

    Map<String, Object> rowUpdate = new HashMap<>();
    rowUpdate.put("name", "Jimmy");
    rowUpdate.put(
        "properties",
        "[ {'key': 'key1', 'value': 'value11' }, {'key': 'key2', 'value' : 'value12'}]");
    rowUpdate.put("events", "[{'key': 123, 'value': false }]");
    String updateResponse =
        testBase.updateRowReturnResponse(
            testBase.endpointPathForRowByPK(testBase.testKeyspaceName(), tableName, rowIdentifier),
            true,
            rowUpdate,
            optimizeMapData);
    Map<String, Object> data = (Map<String, Object>) testBase.readJsonAs(updateResponse, Map.class);
    assertThat(data).containsAllEntriesOf(rowUpdate);

    // Additional processing, checking (compared to "updateRow"):
    Map<String, String> update2 = new HashMap<>();
    update2.put("properties", "[ {'key': 'key1', 'value': 'value11' }]");
    update2.put("events", null);
    testBase.updateRowReturnResponse(
        testBase.endpointPathForRowByPK(testBase.testKeyspaceName(), tableName, rowIdentifier),
        true,
        update2,
        optimizeMapData);

    // And that change actually occurs
    JsonNode json =
        testBase.findRowsAsJsonNode(
            testBase.testKeyspaceName(), tableName, optimizeMapData, rowIdentifier);
    assertThat(json.size()).isEqualTo(1);
    assertThat(json.get(0).get("id").asText()).isEqualTo(rowIdentifier);
    assertThat(json.get(0).get("name").asText()).isEqualTo("Jimmy");
    assertThat(json.get(0).get("properties").get(0).get("key").asText()).isEqualTo("key1");
    assertThat(json.get(0).get("properties").get(0).get("value").asText()).isEqualTo("value11");
    assertThat(json.get(0).get("properties").size()).isEqualTo(1);
    assertTrue(json.get(0).at("/events").isEmpty());
  }

  public static void patchRowWithCompactMap(RestApiV2QIntegrationTestBase testBase) {}

  public static void patchRowWithNonCompactMap(RestApiV2QIntegrationTestBase testBase) {}

  public static void deleteRowWithCompactMap(RestApiV2QIntegrationTestBase testBase) {}

  public static void deleteRowWithNonCompactMap(RestApiV2QIntegrationTestBase testBase) {}

  public static void getRowsWithCompactMap(RestApiV2QIntegrationTestBase testBase) {}

  public static void getRowsWithNonCompactMap(RestApiV2QIntegrationTestBase testBase) {}

  public static void getAllRowsWithCompactMap(RestApiV2QIntegrationTestBase testBase) {}

  public static void getAllRowsWithNonCompactMap(RestApiV2QIntegrationTestBase testBase) {}

  public static void getRowsWithWhereWithCompactMap(RestApiV2QIntegrationTestBase testBase) {}

  public static void getRowsWithWhereWithNonCompactMap(RestApiV2QIntegrationTestBase testBase) {}

  public static void getAllIndexesWithCompactMap(RestApiV2QIntegrationTestBase testBase) {}

  public static void getAllIndexesWithNonCompactMap(RestApiV2QIntegrationTestBase testBase) {}

  public static void findAllTypesWithCompactMap(RestApiV2QIntegrationTestBase testBase) {}

  public static void findAllTypesWithNonCompactMap(RestApiV2QIntegrationTestBase testBase) {}

  public static void findTypeByIdWithCompactMap(RestApiV2QIntegrationTestBase testBase) {}

  public static void findTypeByIdWithNonCompactMap(RestApiV2QIntegrationTestBase testBase) {}

  private static Boolean getFlagForCompactDataTest(boolean serverFlag, boolean testDefault) {
    return serverFlag && testDefault ? null : true;
  }

  private static Boolean getFlagForNonCompactDataTest(boolean serverFlag, boolean testDefault) {
    return !serverFlag && testDefault ? null : false;
  }
}
