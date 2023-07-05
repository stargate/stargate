package io.stargate.sgv2.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class RestApiV2QMapTestsImplIT {
  public static void addRowWithCompactMap(RestApiV2QIntegrationTestBase testBase) {
    final String tableName = testBase.testTableName();
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
    testBase.insertRowWithOptimizeMapFlag(testBase.testKeyspaceName(), tableName, row, true);

    // And verify
    JsonNode readRow =
        testBase.findRowsAsJsonNode(testBase.testKeyspaceName(), tableName, true, "alice");
    assertThat(readRow).hasSize(2);
    assertThat(readRow.at("/count").asInt()).isEqualTo(1);
    assertThat(readRow.at("/data/0/name").asText()).isEqualTo("alice");
    assertThat(readRow.at("/data/0/properties/key1").asText()).isEqualTo("value1");
    assertThat(readRow.at("/data/0/properties/key2").asText()).isEqualTo("value2");
    assertThat(readRow.at("/data/0/events/123").asBoolean()).isEqualTo(true);
    assertThat(readRow.at("/data/0/events/456").asBoolean()).isEqualTo(false);
  }

  public static void addRowWithNonCompactMap(RestApiV2QIntegrationTestBase testBase) {
    final String tableName = testBase.testTableName();
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
    testBase.insertRowWithOptimizeMapFlag(testBase.testKeyspaceName(), tableName, row, false);

    // And verify
    JsonNode readRow =
        testBase.findRowsAsJsonNode(testBase.testKeyspaceName(), tableName, false, "alice");
    assertThat(readRow).hasSize(2);
    assertThat(readRow.at("/count").asInt()).isEqualTo(1);
    assertThat(readRow.at("/data/0/name").asText()).isEqualTo("alice");
    assertThat(readRow.at("/data/0/properties/0/key").asText()).isEqualTo("key1");
    assertThat(readRow.at("/data/0/properties/0/value").asText()).isEqualTo("value1");
    assertThat(readRow.at("/data/0/properties/1/key").asText()).isEqualTo("key2");
    assertThat(readRow.at("/data/0/properties/1/value").asText()).isEqualTo("value2");
    assertThat(readRow.at("/data/0/events/0/key").asInt()).isEqualTo(123);
    assertThat(readRow.at("/data/0/events/0/value").asBoolean()).isEqualTo(true);
    assertThat(readRow.at("/data/0/events/1/key").asInt()).isEqualTo(456);
    assertThat(readRow.at("/data/0/events/1/value").asBoolean()).isEqualTo(false);
  }

  public static void updateRowWithCompactMap(RestApiV2QIntegrationTestBase testBase) {}

  public static void updateRowWithNonCompactMap(RestApiV2QIntegrationTestBase testBase) {}

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
}
