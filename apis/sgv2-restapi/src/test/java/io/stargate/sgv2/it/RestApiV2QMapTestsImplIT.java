package io.stargate.sgv2.it;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import io.stargate.sgv2.api.common.cql.builder.CollectionIndexingType;
import org.apache.http.HttpStatus;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for MAP data type.
 *
 * <p>These tests are run twice, once with the default MAP data type being compact and once with it
 * being non-compact. In order to write the tests only once, the tests are implemented here in this
 * class.
 */
public class RestApiV2QMapTestsImplIT {
  public static void addRowWithCompactMap(
      RestApiV2QIntegrationTestBase testBase, boolean serverFlag, boolean testDefault) {
    Boolean compactMapData = getFlagForCompactDataTest(serverFlag, testDefault);
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
    testBase.insertRow(testBase.testKeyspaceName(), tableName, row, compactMapData);

    // And verify
    JsonNode readRow =
        testBase.findRowsAsJsonNode(
            testBase.testKeyspaceName(), tableName, compactMapData, "alice");
    assertThat(readRow).hasSize(1);
    assertThat(readRow.get(0).get("name").asText()).isEqualTo("alice");
    assertThat(readRow.get(0).get("properties").get("key1").asText()).isEqualTo("value1");
    assertThat(readRow.get(0).get("properties").get("key2").asText()).isEqualTo("value2");
    assertThat(readRow.get(0).get("events").get("123").asBoolean()).isEqualTo(true);
    assertThat(readRow.get(0).get("events").get("456").asBoolean()).isEqualTo(false);
  }

  public static void addRowWithNonCompactMap(
      RestApiV2QIntegrationTestBase testBase, boolean serverFlag, boolean testDefault) {
    Boolean compactMapData = getFlagForNonCompactDataTest(serverFlag, testDefault);
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
    testBase.insertRow(testBase.testKeyspaceName(), tableName, row, compactMapData);

    // And verify
    JsonNode readRow =
        testBase.findRowsAsJsonNode(
            testBase.testKeyspaceName(), tableName, compactMapData, "alice");
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
    Boolean compactMapData = getFlagForCompactDataTest(serverFlag, testDefault);
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
    testBase.insertRow(testBase.testKeyspaceName(), tableName, row, compactMapData);

    Map<String, Object> rowUpdate = new HashMap<>();
    rowUpdate.put("name", "Jimmy");
    rowUpdate.put("properties", "{'key1': 'value11', 'key2': 'value12'}");
    rowUpdate.put("events", "{123: false}");
    String updateResponse =
        testBase.updateRowReturnResponse(
            testBase.endpointPathForRowByPK(testBase.testKeyspaceName(), tableName, rowIdentifier),
            true,
            rowUpdate,
            compactMapData);
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
        compactMapData);

    // And that change actually occurs
    JsonNode json =
        testBase.findRowsAsJsonNode(
            testBase.testKeyspaceName(), tableName, compactMapData, rowIdentifier);
    assertThat(json.size()).isEqualTo(1);
    assertThat(json.get(0).get("id").asText()).isEqualTo(rowIdentifier);
    assertThat(json.get(0).get("name").asText()).isEqualTo("Jimmy");
    assertThat(json.get(0).get("properties").get("key1").asText()).isEqualTo("value11");
    assertTrue(json.get(0).get("properties").at("/key2").isMissingNode());
    assertTrue(json.get(0).at("/events").isEmpty());
  }

  public static void updateRowWithNonCompactMap(
      RestApiV2QIntegrationTestBase testBase, boolean serverFlag, boolean testDefault) {
    Boolean compactMapData = getFlagForNonCompactDataTest(serverFlag, testDefault);
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
    testBase.insertRow(testBase.testKeyspaceName(), tableName, row, compactMapData);

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
            compactMapData);
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
        compactMapData);

    // And that change actually occurs
    JsonNode json =
        testBase.findRowsAsJsonNode(
            testBase.testKeyspaceName(), tableName, compactMapData, rowIdentifier);
    assertThat(json.size()).isEqualTo(1);
    assertThat(json.get(0).get("id").asText()).isEqualTo(rowIdentifier);
    assertThat(json.get(0).get("name").asText()).isEqualTo("Jimmy");
    assertThat(json.get(0).get("properties").get(0).get("key").asText()).isEqualTo("key1");
    assertThat(json.get(0).get("properties").get(0).get("value").asText()).isEqualTo("value11");
    assertThat(json.get(0).get("properties").size()).isEqualTo(1);
    assertTrue(json.get(0).at("/events").isEmpty());
  }

  public static void patchRowWithCompactMap(
      RestApiV2QIntegrationTestBase testBase, boolean serverFlag, boolean testDefault) {
    Boolean compactMapData = getFlagForCompactDataTest(serverFlag, testDefault);
    final String tableName = testBase.testTableName() + (testDefault ? "1" : "2");
    testBase.createTestTable(
        testBase.testKeyspaceName(),
        tableName,
        Arrays.asList(
            "id uuid", "name text", "properties map<text,text>", "events map<int,boolean>"),
        Arrays.asList("id"),
        null);

    String rowIdentifier = UUID.randomUUID().toString();
    Map<String, String> row = new HashMap<>();
    row.put("id", rowIdentifier);
    row.put("name", "John");
    row.put("properties", "{'key1': 'value1', 'key2': 'value2'}");
    row.put("events", "{123: true, 456: false}");
    testBase.insertRow(testBase.testKeyspaceName(), tableName, row, compactMapData);

    Map<String, String> rowUpdate = new HashMap<>();
    rowUpdate.put("name", "Jimmy");
    rowUpdate.put("properties", "{'key1': 'value11'}");
    rowUpdate.put("events", "{123: false}");

    String patchResponse =
        testBase.patchRowReturnResponse(
            testBase.endpointPathForRowByPK(testBase.testKeyspaceName(), tableName, rowIdentifier),
            false,
            rowUpdate,
            compactMapData);
    Map<String, String> data = testBase.readWrappedRESTResponse(patchResponse, Map.class);
    assertThat(data).containsAllEntriesOf(rowUpdate);

    JsonNode json =
        testBase.findRowsAsJsonNode(
            testBase.testKeyspaceName(), tableName, compactMapData, rowIdentifier);
    assertThat(json.size()).isEqualTo(1);
    assertThat(json.get(0).get("id").asText()).isEqualTo(rowIdentifier);
    assertThat(json.get(0).get("name").asText()).isEqualTo("Jimmy");
    assertThat(json.get(0).get("properties").get("key1").asText()).isEqualTo("value11");
    assertTrue(json.get(0).get("properties").at("/key2").isMissingNode());
    assertThat(json.get(0).get("events").get("123").asBoolean()).isFalse();
  }

  public static void patchRowWithNonCompactMap(
      RestApiV2QIntegrationTestBase testBase, boolean serverFlag, boolean testDefault) {
    Boolean compactMapData = getFlagForNonCompactDataTest(serverFlag, testDefault);
    final String tableName = testBase.testTableName() + (testDefault ? "1" : "2");
    testBase.createTestTable(
        testBase.testKeyspaceName(),
        tableName,
        Arrays.asList(
            "id uuid", "name text", "properties map<text,text>", "events map<int,boolean>"),
        Arrays.asList("id"),
        null);

    String rowIdentifier = UUID.randomUUID().toString();
    Map<String, String> row = new HashMap<>();
    row.put("id", rowIdentifier);
    row.put("name", "John");
    row.put(
        "properties", "[{'key': 'key1', 'value': 'value1' }, {'key': 'key2', 'value' : 'value2'}]");
    row.put("events", "[{'key': 123, 'value': true }, {'key': 456, 'value' : false}]");
    testBase.insertRow(testBase.testKeyspaceName(), tableName, row, compactMapData);

    Map<String, String> rowUpdate = new HashMap<>();
    rowUpdate.put("name", "Jimmy");
    rowUpdate.put("properties", "[{'key': 'key1', 'value': 'value11' }]");
    rowUpdate.put("events", "[{'key': 123, 'value': false }]");

    String patchResponse =
        testBase.patchRowReturnResponse(
            testBase.endpointPathForRowByPK(testBase.testKeyspaceName(), tableName, rowIdentifier),
            false,
            rowUpdate,
            compactMapData);
    Map<String, String> data = testBase.readWrappedRESTResponse(patchResponse, Map.class);
    assertThat(data).containsAllEntriesOf(rowUpdate);

    JsonNode json =
        testBase.findRowsAsJsonNode(
            testBase.testKeyspaceName(), tableName, compactMapData, rowIdentifier);
    assertThat(json.size()).isEqualTo(1);
    assertThat(json.get(0).get("id").asText()).isEqualTo(rowIdentifier);
    assertThat(json.get(0).get("name").asText()).isEqualTo("Jimmy");
    assertThat(json.get(0).get("properties").get(0).get("key").asText()).isEqualTo("key1");
    assertThat(json.get(0).get("properties").get(0).get("value").asText()).isEqualTo("value11");
    assertThat(json.get(0).get("properties").size()).isEqualTo(1);
    assertThat(json.get(0).get("events").get(0).get("key").asInt()).isEqualTo(123);
    assertThat(json.get(0).get("events").get(0).get("value").asBoolean()).isFalse();
  }

  public static void getRowsWithCompactMap(
      RestApiV2QIntegrationTestBase testBase, boolean serverFlag, boolean testDefault) {
    Boolean compactMapData = getFlagForCompactDataTest(serverFlag, testDefault);
    final String tableName = testBase.testTableName() + (testDefault ? "1" : "2");
    testBase.createTestTable(
        testBase.testKeyspaceName(),
        tableName,
        Arrays.asList(
            "name text",
            "properties map<text,text>",
            "events map<int,boolean>",
            "pairs map<text,tuple<boolean,int>>"),
        Arrays.asList("name"),
        null);

    Map<String, String> row = new HashMap<>();
    row.put("name", "alice");
    row.put("properties", "{'key1': 'value1', 'key2': 'value2'}");
    row.put("events", "{123: true, 456: false}");
    row.put("pairs", "{'key1': (true, 1), 'key2': (false, 2)}");
    testBase.insertRow(testBase.testKeyspaceName(), tableName, row, compactMapData);

    // And verify
    JsonNode readRow =
        testBase.findRowsAsJsonNode(
            testBase.testKeyspaceName(), tableName, compactMapData, "alice");
    assertThat(readRow).hasSize(1);
    assertThat(readRow.get(0).get("name").asText()).isEqualTo("alice");
    assertThat(readRow.get(0).get("properties").get("key1").asText()).isEqualTo("value1");
    assertThat(readRow.get(0).get("properties").get("key2").asText()).isEqualTo("value2");
    assertThat(readRow.get(0).get("events").get("123").asBoolean()).isEqualTo(true);
    assertThat(readRow.get(0).get("events").get("456").asBoolean()).isEqualTo(false);
    assertThat(readRow.get(0).get("pairs").get("key1").get(0).asBoolean()).isEqualTo(true);
    assertThat(readRow.get(0).get("pairs").get("key1").get(1).asInt()).isEqualTo(1);
    assertThat(readRow.get(0).get("pairs").get("key2").get(0).asBoolean()).isEqualTo(false);
    assertThat(readRow.get(0).get("pairs").get("key2").get(1).asInt()).isEqualTo(2);
  }

  public static void getRowsWithNonCompactMap(
      RestApiV2QIntegrationTestBase testBase, boolean serverFlag, boolean testDefault) {
    Boolean compactMapData = getFlagForNonCompactDataTest(serverFlag, testDefault);
    final String tableName = testBase.testTableName() + (testDefault ? "1" : "2");
    testBase.createTestTable(
        testBase.testKeyspaceName(),
        tableName,
        Arrays.asList(
            "name text",
            "properties map<text,text>",
            "events map<int,boolean>",
            "pairs map<text,tuple<boolean,int>>"),
        Arrays.asList("name"),
        null);

    Map<String, String> row = new HashMap<>();
    row.put("name", "alice");
    row.put(
        "properties", "[{'key': 'key1', 'value': 'value1' }, {'key': 'key2', 'value' : 'value2'}]");
    row.put("events", "[{'key': 123, 'value': true }, {'key': 456, 'value' : false}]");
    row.put(
        "pairs", "[{'key': 'key1', 'value': (true, 1) }, {'key': 'key2', 'value' : (false, 2)}]");
    testBase.insertRow(testBase.testKeyspaceName(), tableName, row, compactMapData);

    // And verify
    JsonNode readRow =
        testBase.findRowsAsJsonNode(
            testBase.testKeyspaceName(), tableName, compactMapData, "alice");
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
    assertThat(readRow.get(0).get("pairs").get(0).get("key").asText()).isEqualTo("key1");
    assertThat(readRow.get(0).get("pairs").get(0).get("value").get(0).asBoolean()).isEqualTo(true);
    assertThat(readRow.get(0).get("pairs").get(0).get("value").get(1).asInt()).isEqualTo(1);
    assertThat(readRow.get(0).get("pairs").get(1).get("key").asText()).isEqualTo("key2");
    assertThat(readRow.get(0).get("pairs").get(1).get("value").get(0).asBoolean()).isEqualTo(false);
    assertThat(readRow.get(0).get("pairs").get(1).get("value").get(1).asInt()).isEqualTo(2);
  }

  public static void getAllRowsWithCompactMap(
      RestApiV2QIntegrationTestBase testBase, boolean serverFlag, boolean testDefault) {
    Boolean compactMapData = getFlagForCompactDataTest(serverFlag, testDefault);
    final String tableName = testBase.testTableName() + (testDefault ? "1" : "2");
    testBase.createTestTable(
        testBase.testKeyspaceName(),
        tableName,
        Arrays.asList(
            "id uuid",
            "id2 text",
            "name text",
            "properties map<text,text>",
            "events map<int,boolean>"),
        List.of("id"),
        List.of("id2"));
    String mainKey = "113fbac2-0cad-40f8-940c-6a95f8d1a4cf";
    String secondKey = "113fbac2-0cad-40f8-940c-6a95f8d1afff";
    testBase.insertRows(
        testBase.testKeyspaceName(),
        tableName,
        Arrays.asList(
            Arrays.asList(
                "id " + mainKey,
                "id2 a",
                "name Bob",
                "properties {'key11': 'value11', 'key12': 'value12'}",
                "events {123: true, 456: false}"),
            Arrays.asList(
                "id " + mainKey,
                "id2 b",
                "name Joe",
                "properties {'key21': 'value21', 'key22': 'value22'}",
                "events {789: true, 101112: false}"),
            Arrays.asList(
                "id " + mainKey,
                "id2 x",
                "name Patrick",
                "properties {'key31': 'value31', 'key32': 'value32'}",
                "events {131415: false, 161718: false}"),
            Arrays.asList(
                "id " + secondKey,
                "id2 e",
                "name Alice",
                "properties {'key41': 'value41', 'key42': 'value42'}",
                "events {192021: true, 222324: true}")),
        compactMapData);

    // get first page; cannot use "raw" mode as we need pagingState
    String whereClause = String.format("{\"id\":{\"$eq\":\"%s\"}}", mainKey);
    final String path = testBase.endpointPathForRowGetWith(testBase.testKeyspaceName(), tableName);
    RequestSpecification requestSpecification = testBase.givenWithAuth();
    if (compactMapData != null) {
      requestSpecification.queryParam("compactMapData", compactMapData);
    }
    String response =
        requestSpecification
            .queryParam("page-size", 2)
            .queryParam("where", whereClause)
            .when()
            .get(path)
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();

    JsonNode json = testBase.readJsonAsTree(response);
    assertThat(json.at("/count").intValue()).isEqualTo(2);
    JsonNode data = json.at("/data");
    assertThat(data.size()).isEqualTo(2);
    assertThat(data.at("/0/id").asText()).isEqualTo(mainKey);
    assertThat(data.at("/0/id2").asText()).isEqualTo("a");
    assertThat(data.at("/0/name").asText()).isEqualTo("Bob");
    assertThat(data.at("/0/properties/key11").asText()).isEqualTo("value11");
    assertThat(data.at("/0/properties/key12").asText()).isEqualTo("value12");
    assertThat(data.at("/0/events/123").asBoolean()).isTrue();
    assertThat(data.at("/0/events/456").asBoolean()).isFalse();
    assertThat(data.at("/1/id").asText()).isEqualTo(mainKey);
    assertThat(data.at("/1/id2").asText()).isEqualTo("b");
    assertThat(data.at("/1/name").asText()).isEqualTo("Joe");
    assertThat(data.at("/1/properties/key21").asText()).isEqualTo("value21");
    assertThat(data.at("/1/properties/key22").asText()).isEqualTo("value22");
    assertThat(data.at("/1/events/789").asBoolean()).isTrue();
    assertThat(data.at("/1/events/101112").asBoolean()).isFalse();

    String pagingState = json.at("/pageState").asText();
    assertThat(pagingState).isNotEmpty();
    requestSpecification = testBase.givenWithAuth();
    if (compactMapData != null) {
      requestSpecification.queryParam("compactMapData", compactMapData);
    }
    response =
        requestSpecification
            .queryParam("page-size", 99)
            .queryParam("where", whereClause)
            .queryParam("page-state", pagingState)
            .when()
            .get(path)
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();

    json = testBase.readJsonAsTree(response);
    assertThat(json.at("/count").intValue()).isEqualTo(1);
    data = json.at("/data");
    assertThat(data.size()).isEqualTo(1);
    assertThat(data.at("/0/id").asText()).isEqualTo(mainKey);
    assertThat(data.at("/0/id2").asText()).isEqualTo("x");
    assertThat(data.at("/0/name").asText()).isEqualTo("Patrick");
    assertThat(data.at("/0/properties/key31").asText()).isEqualTo("value31");
    assertThat(data.at("/0/properties/key32").asText()).isEqualTo("value32");
    assertThat(data.at("/0/events/131415").asBoolean()).isFalse();
    assertThat(data.at("/0/events/161718").asBoolean()).isFalse();
    // Either missing or empty String
    assertThat(json.at("/pageState").asText()).isEmpty();
  }

  public static void getAllRowsWithNonCompactMap(
      RestApiV2QIntegrationTestBase testBase, boolean serverFlag, boolean testDefault) {
    Boolean compactMapData = getFlagForNonCompactDataTest(serverFlag, testDefault);
    final String tableName = testBase.testTableName() + (testDefault ? "1" : "2");
    testBase.createTestTable(
        testBase.testKeyspaceName(),
        tableName,
        Arrays.asList(
            "id uuid",
            "id2 text",
            "name text",
            "properties map<text,text>",
            "events map<int,boolean>"),
        List.of("id"),
        List.of("id2"));
    String mainKey = "113fbac2-0cad-40f8-940c-6a95f8d1a4cf";
    String secondKey = "113fbac2-0cad-40f8-940c-6a95f8d1afff";
    testBase.insertRows(
        testBase.testKeyspaceName(),
        tableName,
        Arrays.asList(
            Arrays.asList(
                "id " + mainKey,
                "id2 a",
                "name Bob",
                "properties [{'key': 'key11', 'value': 'value11'}, {'key': 'key12', 'value': 'value12'}]",
                "events [ { 'key': 123, 'value': true }, { 'key': 456, 'value': false } ]"),
            Arrays.asList(
                "id " + mainKey,
                "id2 b",
                "name Joe",
                "properties [{'key': 'key21', 'value': 'value21'}, { 'key': 'key22', 'value': 'value22'}]",
                "events [ { 'key': 789, 'value': true }, { 'key': 101112, 'value': false } ]"),
            Arrays.asList(
                "id " + mainKey,
                "id2 x",
                "name Patrick",
                "properties [{'key': 'key31', 'value': 'value31'}, { 'key': 'key32', 'value': 'value32'}]",
                "events [ { 'key': 131415, 'value': false }, { 'key': 161718, 'value': true } ]"),
            Arrays.asList(
                "id " + secondKey,
                "id2 e",
                "name Alice",
                "properties [{'key': 'key41', 'value': 'value41'}, { 'key': 'key42', 'value': 'value42'}]",
                "events [ { 'key': 192021, 'value': false }, { 'key': 222324, 'value': false } ]")),
        compactMapData);

    // get first page; cannot use "raw" mode as we need pagingState
    String whereClause = String.format("{\"id\":{\"$eq\":\"%s\"}}", mainKey);
    final String path = testBase.endpointPathForRowGetWith(testBase.testKeyspaceName(), tableName);
    RequestSpecification requestSpecification = testBase.givenWithAuth();
    if (compactMapData != null) {
      requestSpecification.queryParam("compactMapData", compactMapData);
    }
    String response =
        requestSpecification
            .queryParam("page-size", 2)
            .queryParam("where", whereClause)
            .when()
            .get(path)
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();

    JsonNode json = testBase.readJsonAsTree(response);
    assertThat(json.at("/count").intValue()).isEqualTo(2);
    JsonNode data = json.at("/data");
    assertThat(data.size()).isEqualTo(2);
    assertThat(data.at("/0/id").asText()).isEqualTo(mainKey);
    assertThat(data.at("/0/id2").asText()).isEqualTo("a");
    assertThat(data.at("/0/name").asText()).isEqualTo("Bob");
    assertThat(data.at("/0/properties/0/key").asText()).isEqualTo("key11");
    assertThat(data.at("/0/properties/0/value").asText()).isEqualTo("value11");
    assertThat(data.at("/0/properties/1/key").asText()).isEqualTo("key12");
    assertThat(data.at("/0/properties/1/value").asText()).isEqualTo("value12");
    assertThat(data.at("/0/events/0/key").asInt()).isEqualTo(123);
    assertThat(data.at("/0/events/0/value").asBoolean()).isTrue();
    assertThat(data.at("/0/events/1/key").asInt()).isEqualTo(456);
    assertThat(data.at("/0/events/1/value").asBoolean()).isFalse();
    assertThat(data.at("/1/id").asText()).isEqualTo(mainKey);
    assertThat(data.at("/1/id2").asText()).isEqualTo("b");
    assertThat(data.at("/1/name").asText()).isEqualTo("Joe");
    assertThat(data.at("/1/properties/0/key").asText()).isEqualTo("key21");
    assertThat(data.at("/1/properties/0/value").asText()).isEqualTo("value21");
    assertThat(data.at("/1/properties/1/key").asText()).isEqualTo("key22");
    assertThat(data.at("/1/properties/1/value").asText()).isEqualTo("value22");
    assertThat(data.at("/1/events/0/key").asInt()).isEqualTo(789);
    assertThat(data.at("/1/events/0/value").asBoolean()).isTrue();
    assertThat(data.at("/1/events/1/key").asInt()).isEqualTo(101112);
    assertThat(data.at("/1/events/1/value").asBoolean()).isFalse();

    String pagingState = json.at("/pageState").asText();
    assertThat(pagingState).isNotEmpty();
    requestSpecification = testBase.givenWithAuth();
    if (compactMapData != null) {
      requestSpecification.queryParam("compactMapData", compactMapData);
    }
    response =
        requestSpecification
            .queryParam("page-size", 99)
            .queryParam("where", whereClause)
            .queryParam("page-state", pagingState)
            .when()
            .get(path)
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();

    json = testBase.readJsonAsTree(response);
    assertThat(json.at("/count").intValue()).isEqualTo(1);
    data = json.at("/data");
    assertThat(data.size()).isEqualTo(1);
    assertThat(data.at("/0/id").asText()).isEqualTo(mainKey);
    assertThat(data.at("/0/id2").asText()).isEqualTo("x");
    assertThat(data.at("/0/name").asText()).isEqualTo("Patrick");
    assertThat(data.at("/0/properties/0/key").asText()).isEqualTo("key31");
    assertThat(data.at("/0/properties/0/value").asText()).isEqualTo("value31");
    assertThat(data.at("/0/properties/1/key").asText()).isEqualTo("key32");
    assertThat(data.at("/0/properties/1/value").asText()).isEqualTo("value32");
    assertThat(data.at("/0/events/0/key").asInt()).isEqualTo(131415);
    assertThat(data.at("/0/events/0/value").asBoolean()).isFalse();
    assertThat(data.at("/0/events/1/key").asInt()).isEqualTo(161718);
    assertThat(data.at("/0/events/1/value").asBoolean()).isTrue();
    // Either missing or empty String
    assertThat(json.at("/pageState").asText()).isEmpty();
  }

  public static void getRowsWithWhereWithCompactMap(
      RestApiV2QIntegrationTestBase testBase, boolean serverFlag, boolean testDefault) {
    Boolean compactMapData = getFlagForCompactDataTest(serverFlag, testDefault);
    final String tableName = testBase.testTableName() + (testDefault ? "1" : "2");
    final String udtName = "testUDT" + (testDefault ? "1" : "2") + (serverFlag ? "1" : "2") + "_c";
    String udtCreate =
        "{\"name\": \""
            + udtName
            + "\", \"fields\":"
            + "[{\"name\":\"name\",\"typeDefinition\":\"text\"},"
            + "{\"name\":\"age\",\"typeDefinition\":\"int\"}]}";
    testBase
        .givenWithAuth()
        .contentType(ContentType.JSON)
        .body(udtCreate)
        .when()
        .post(testBase.endpointPathForUDTAdd(testBase.testKeyspaceName()))
        .then()
        .statusCode(HttpStatus.SC_CREATED);
    testBase.createTestTable(
        testBase.testKeyspaceName(),
        tableName,
        Arrays.asList(
            "id text",
            "attributes map<text,text>",
            "firstName text",
            "info1 map<text,frozen<" + udtName + ">>"),
        Arrays.asList("id"),
        Arrays.asList("firstName"));
    // Cannot query against non-key columns, unless there's an index, so:
    testBase.createTestIndex(
        testBase.testKeyspaceName(),
        tableName,
        "attributes",
        "attributes_mapentry_index" + (serverFlag ? "1" : "2") + (testDefault ? "1" : "2"),
        false,
        CollectionIndexingType.ENTRIES);

    testBase.insertTypedRows(
        testBase.testKeyspaceName(),
        tableName,
        Arrays.asList(
            testBase.map("id", 1, "firstName", "Bob", "attributes", testBase.map("a", "1")),
            testBase.map("id", 1, "firstName", "Dave", "attributes", testBase.map("b", "2")),
            Map.of(
                "id",
                1,
                "firstName",
                "Fred",
                "attributes",
                testBase.map("c", "3"),
                "info1",
                testBase.map("a", testBase.map("name", "Bob", "age", 42)))),
        compactMapData);

    // First, no match
    String noMatchesClause =
        "{\"id\":{\"$eq\":\"1\"},\"attributes\":{\"$containsEntry\":{\"key\":\"b\",\"value\":\"1\"}}}";
    ArrayNode rows =
        testBase.findRowsWithWhereAsJsonNode(
            testBase.testKeyspaceName(), tableName, noMatchesClause, compactMapData);
    assertThat(rows).hasSize(0);

    // and then a single match
    String matchingClause =
        "{\"id\":{\"$eq\":\"1\"},\"attributes\":{\"$containsEntry\":{\"key\":\"c\",\"value\":\"3\"}}}";
    rows =
        testBase.findRowsWithWhereAsJsonNode(
            testBase.testKeyspaceName(), tableName, matchingClause, compactMapData);
    assertThat(rows).hasSize(1);
    assertThat(rows.at("/0/firstName").asText()).isEqualTo("Fred");
    // Also verify how Map values serialized (see [stargate#2577])
    assertThat(rows.at("/0/attributes")).isEqualTo(testBase.readJsonAsTree("{\"c\":\"3\"}"));
    assertThat(rows.at("/0/info1"))
        .isEqualTo(testBase.readJsonAsTree("{\"a\":{\"name\":\"Bob\",\"age\":42}}"));
  }

  public static void getRowsWithWhereWithNonCompactMap(
      RestApiV2QIntegrationTestBase testBase, boolean serverFlag, boolean testDefault) {
    Boolean compactMapData = getFlagForNonCompactDataTest(serverFlag, testDefault);
    final String tableName = testBase.testTableName() + (testDefault ? "1" : "2");
    final String udtName = "testUDT" + (testDefault ? "1" : "2") + (serverFlag ? "1" : "2") + "_nc";
    String udtCreate =
        "{\"name\": \""
            + udtName
            + "\", \"fields\":"
            + "[{\"name\":\"name\",\"typeDefinition\":\"text\"},"
            + "{\"name\":\"age\",\"typeDefinition\":\"int\"}]}";
    testBase
        .givenWithAuth()
        .contentType(ContentType.JSON)
        .body(udtCreate)
        .when()
        .post(testBase.endpointPathForUDTAdd(testBase.testKeyspaceName()))
        .then()
        .statusCode(HttpStatus.SC_CREATED);
    testBase.createTestTable(
        testBase.testKeyspaceName(),
        tableName,
        Arrays.asList(
            "id text",
            "attributes map<text,int>",
            "firstName text",
            "info1 map<text,frozen<" + udtName + ">>"),
        List.of("id"),
        List.of("firstName"));
    // Cannot query against non-key columns, unless there's an index, so:
    testBase.createTestIndex(
        testBase.testKeyspaceName(),
        tableName,
        "attributes",
        "attributes_mapentry_index" + (testDefault ? "1" : "2"),
        false,
        CollectionIndexingType.ENTRIES);

    testBase.insertTypedRows(
        testBase.testKeyspaceName(),
        tableName,
        Arrays.asList(
            testBase.map(
                "id",
                1,
                "firstName",
                "Bob",
                "attributes",
                testBase.list(testBase.map("key", "a", "value", 1))),
            testBase.map(
                "id",
                1,
                "firstName",
                "Dave",
                "attributes",
                testBase.list(testBase.map("key", "b", "value", 2))),
            Map.of(
                "id",
                1,
                "firstName",
                "Fred",
                "attributes",
                testBase.list(testBase.map("key", "c", "value", 3)),
                "info1",
                testBase.list(
                    testBase.map("key", "a", "value", testBase.map("name", "Bob", "age", 42))))),
        compactMapData);

    // First, no match
    String noMatchesClause =
        "{\"id\":{\"$eq\":\"1\"},\"attributes\":{\"$containsEntry\":{\"key\":\"b\",\"value\":1}}}";
    ArrayNode rows =
        testBase.findRowsWithWhereAsJsonNode(
            testBase.testKeyspaceName(), tableName, noMatchesClause, compactMapData);
    assertThat(rows).hasSize(0);

    // and then a single match
    String matchingClause =
        "{\"id\":{\"$eq\":\"1\"},\"attributes\":{\"$containsEntry\":{\"key\":\"c\",\"value\":3}}}";
    rows =
        testBase.findRowsWithWhereAsJsonNode(
            testBase.testKeyspaceName(), tableName, matchingClause, compactMapData);
    assertThat(rows).hasSize(1);
    assertThat(rows.at("/0/firstName").asText()).isEqualTo("Fred");
    // Also verify how Map values serialized (see [stargate#2577])
    assertThat(rows.at("/0/attributes"))
        .isEqualTo(testBase.readJsonAsTree("[{\"key\":\"c\", \"value\":3}]"));
    assertThat(rows.at("/0/info1"))
        .isEqualTo(
            testBase.readJsonAsTree("[{\"key\":\"a\", \"value\":{\"name\":\"Bob\",\"age\":42}}]"));
  }

  public static void getAllIndexesWithCompactMap(
      RestApiV2QIntegrationTestBase testBase, boolean serverFlag, boolean testDefault) {
    Boolean compactMapData = getFlagForCompactDataTest(serverFlag, testDefault);
    final String tableName = testBase.testTableName() + (testDefault ? "1" : "2");
    testBase.createTestTable(
        testBase.testKeyspaceName(),
        tableName,
        Arrays.asList("id text", "attributes map<text,text>", "firstName text"),
        List.of("id"),
        List.of("firstName"));

    String indexName = "attributes_mapentry_idx" + "_" + serverFlag + "_" + testDefault + "_c";
    testBase.createTestIndex(
        testBase.testKeyspaceName(),
        tableName,
        "attributes",
        indexName,
        false,
        CollectionIndexingType.ENTRIES);
    String response =
        testBase.getAllIndexes(testBase.testKeyspaceName(), tableName, compactMapData);
    List<RestApiV2QSchemaIndexesIT.IndexDesc> indexList =
        Arrays.asList(testBase.readJsonAs(response, RestApiV2QSchemaIndexesIT.IndexDesc[].class));
    assertThat(indexList).hasSize(1);
    assertThat(indexList.get(0).index_name()).isEqualTo(indexName);
    assertThat(indexList.get(0).options().get("target")).isEqualTo("entries(attributes)");
  }

  public static void getAllIndexesWithNonCompactMap(
      RestApiV2QIntegrationTestBase testBase, boolean serverFlag, boolean testDefault) {
    Boolean compactMapData = getFlagForNonCompactDataTest(serverFlag, testDefault);
    final String tableName = testBase.testTableName() + (testDefault ? "1" : "2");
    testBase.createTestTable(
        testBase.testKeyspaceName(),
        tableName,
        Arrays.asList("id text", "attributes map<text,text>", "firstName text"),
        List.of("id"),
        List.of("firstName"));

    String indexName = "attributes_mapentry_idx" + "_" + serverFlag + "_" + testDefault + "_nc";
    testBase.createTestIndex(
        testBase.testKeyspaceName(),
        tableName,
        "attributes",
        indexName,
        false,
        CollectionIndexingType.ENTRIES);
    String response =
        testBase.getAllIndexes(testBase.testKeyspaceName(), tableName, compactMapData);
    List<RestApiV2QSchemaIndexesIT.IndexDescOptionsAsList> indexList =
        Arrays.asList(
            testBase.readJsonAs(
                response, RestApiV2QSchemaIndexesIT.IndexDescOptionsAsList[].class));
    assertThat(indexList).hasSize(1);
    assertThat(indexList.get(0).index_name()).isEqualTo(indexName);
    assertThat(indexList.get(0).options().get(0).get("key")).isEqualTo("target");
    assertThat(indexList.get(0).options().get(0).get("value")).isEqualTo("entries(attributes)");
  }

  private static Boolean getFlagForCompactDataTest(boolean serverFlag, boolean testDefault) {
    return serverFlag && testDefault ? null : true;
  }

  private static Boolean getFlagForNonCompactDataTest(boolean serverFlag, boolean testDefault) {
    return !serverFlag && testDefault ? null : false;
  }
}
