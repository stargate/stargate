package io.stargate.sgv2.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@QuarkusTestResource(StargateTestResource.class)
public class RestApiV2QMapOptimizedDisabledIT extends RestApiV2QIntegrationTestBase {
  public static class Profile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of("stargate.rest.optimize-map-data", "false");
    }
  }

  public RestApiV2QMapOptimizedDisabledIT() {
    super("rowadd_ks_", "rowadd_t_", KeyspaceCreation.PER_CLASS);
  }

  @Test
  public void addRowWithCompactMap() {
    final String tableName = testTableName();
    createTestTable(
        testKeyspaceName(),
        tableName,
        Arrays.asList("name text", "properties map<text,text>"),
        Arrays.asList("name"),
        null);

    Map<String, String> row = new HashMap<>();
    row.put("name", "alice");
    row.put("properties", "{'key1': 'value1', 'key2': 'value2'}");
    insertRowWithOptimizeMapFlag(testKeyspaceName(), tableName, row, true);

    // And verify
    JsonNode readRow = findRowsAsJsonNode(testKeyspaceName(), tableName, true, "alice");
    assertThat(readRow).hasSize(2);
    assertThat(readRow.at("/count").asInt()).isEqualTo(1);
    assertThat(readRow.at("/data/0/name").asText()).isEqualTo("alice");
    assertThat(readRow.at("/data/0/properties/key1").asText()).isEqualTo("value1");
    assertThat(readRow.at("/data/0/properties/key2").asText()).isEqualTo("value2");
  }

  @Test
  public void addRowWithNonCompactMap() {
    final String tableName = testTableName();
    createTestTable(
        testKeyspaceName(),
        tableName,
        Arrays.asList("name text", "properties map<text,text>"),
        Arrays.asList("name"),
        null);

    Map<String, String> row = new HashMap<>();
    row.put("name", "alice");
    row.put(
        "properties", "[{'key': 'key1', 'value': 'value1' }, {'key': 'key2', 'value' : 'value2'}]");
    insertRowWithOptimizeMapFlag(testKeyspaceName(), tableName, row, false);

    // And verify
    JsonNode readRow = findRowsAsJsonNode(testKeyspaceName(), tableName, false, "alice");
    assertThat(readRow).hasSize(2);
    assertThat(readRow.at("/count").asInt()).isEqualTo(1);
    assertThat(readRow.at("/data/0/name").asText()).isEqualTo("alice");
    assertThat(readRow.at("/data/0/properties/0/key").asText()).isEqualTo("key1");
    assertThat(readRow.at("/data/0/properties/0/value").asText()).isEqualTo("value1");
    assertThat(readRow.at("/data/0/properties/1/key").asText()).isEqualTo("key2");
    assertThat(readRow.at("/data/0/properties/1/value").asText()).isEqualTo("value2");
  }
}
