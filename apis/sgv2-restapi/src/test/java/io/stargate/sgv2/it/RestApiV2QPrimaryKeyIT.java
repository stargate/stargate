package io.stargate.sgv2.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.Test;

/**
 * Tests that exercise use of "special" characters for primary key PathParam; especially to ensure
 * that escaped slashes are not decoded prematurely.
 */
@QuarkusIntegrationTest
@QuarkusTestResource(StargateTestResource.class)
public class RestApiV2QPrimaryKeyIT extends RestApiV2QIntegrationTestBase {
  public RestApiV2QPrimaryKeyIT() {
    super("rowpk_ks_", "rowpk_t_", KeyspaceCreation.PER_CLASS);
  }

  @Test
  public void getWithSpecialCharsInPK() {
    final String tableName = testTableName();
    final String testKey1 = "get1/ab&cd/xy";
    final String testKey2 = "get2/ef&gh/yz";

    createPKTestTable(tableName);

    // To try to ensure we actually find the right entry, create one other entry first
    insertRow(
        testKeyspaceName(),
        tableName,
        Map.of("id1", "other", "id2", "key", "description", "Desc 2"));

    // and then the row we are actually looking for:
    insertRow(
        testKeyspaceName(),
        tableName,
        Map.of("id1", testKey1, "id2", testKey2, "description", "Desc"));

    String response =
        givenWithAuth()
            .queryParam("raw", "false")
            .when()
            .get(endpointTemplateForRowByPK(testKeyspaceName(), tableName, 2), testKey1, testKey2)
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    ListOfMapsGetResponseWrapper wrapper = readJsonAs(response, ListOfMapsGetResponseWrapper.class);

    // Verify we fetch one and only one entry
    assertThat(wrapper.count()).isEqualTo(1);
    List<Map<String, Object>> data = wrapper.data();
    assertThat(data.size()).isEqualTo(1);
    // and that its contents match
    assertThat(data.get(0).get("id1")).isEqualTo(testKey1);
    assertThat(data.get(0).get("id2")).isEqualTo(testKey2);
    assertThat(data.get(0).get("description")).isEqualTo("Desc");
  }

  @Test
  public void updateWithSpecialCharsInPK() {
    final String tableName = testTableName();
    final String testKey1 = "put1/ab&cd/xy";
    final String testKey2 = "put2/ef&gh/yz";

    createPKTestTable(tableName);

    insertRow(
        testKeyspaceName(),
        tableName,
        Map.of("id1", testKey1, "id2", testKey2, "description", "Desc"));

    Map<String, String> rowUpdate = Map.of("description", "Updated Desc");
    final String payloadJSON = asJsonString(rowUpdate);
    givenWithAuth()
        .queryParam("raw", false)
        .contentType(ContentType.JSON)
        .body(payloadJSON)
        .when()
        .put(endpointTemplateForRowByPK(testKeyspaceName(), tableName, 2), testKey1, testKey2)
        .then()
        .statusCode(HttpStatus.SC_OK);

    List<Map<String, Object>> rows = findRows(testKeyspaceName(), tableName, testKey1, testKey2);
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).get("id1")).isEqualTo(testKey1);
    assertThat(rows.get(0).get("id2")).isEqualTo(testKey2);
    assertThat(rows.get(0).get("description")).isEqualTo("Updated Desc");
  }

  @Test
  public void patchWithSpecialCharsInPK() {
    final String tableName = testTableName();
    final String testKey1 = "patch1/ab&cd/xy";
    final String testKey2 = "patch2/ef&gh/yz";
    createPKTestTable(tableName);

    insertRow(
        testKeyspaceName(),
        tableName,
        Map.of("id1", testKey1, "id2", testKey2, "description", "Desc"));

    Map<String, String> rowUpdate = Map.of("description", "Patched Desc");
    final String payloadJSON = asJsonString(rowUpdate);
    givenWithAuth()
        .queryParam("raw", true)
        .contentType(ContentType.JSON)
        .body(payloadJSON)
        .when()
        .patch(endpointTemplateForRowByPK(testKeyspaceName(), tableName, 2), testKey1, testKey2)
        .then()
        .statusCode(HttpStatus.SC_OK);

    List<Map<String, Object>> rows = findRows(testKeyspaceName(), tableName, testKey1, testKey2);
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).get("id1")).isEqualTo(testKey1);
    assertThat(rows.get(0).get("id2")).isEqualTo(testKey2);
    assertThat(rows.get(0).get("description")).isEqualTo("Patched Desc");
  }

  @Test
  public void deleteWithSpecialCharsInPK() {
    final String tableName = testTableName();
    final String testKey1 = "delete1/ab&cd/xy";
    final String testKey2 = "delete2/ef&gh/yz";
    createPKTestTable(tableName);

    insertRow(
        testKeyspaceName(),
        tableName,
        Map.of("id1", testKey1, "id2", testKey2, "description", "Desc"));

    givenWithAuth()
        .when()
        .delete(endpointTemplateForRowByPK(testKeyspaceName(), tableName, 2), testKey1, testKey2)
        .then()
        .statusCode(HttpStatus.SC_NO_CONTENT);
    assertThat(findRows(testKeyspaceName(), tableName, testKey1, testKey2)).hasSize(0);
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Helper methods
  /////////////////////////////////////////////////////////////////////////
   */

  private void createPKTestTable(String tableName) {
    createTestTable(
        testKeyspaceName(),
        tableName,
        Arrays.asList("id1 text", "id2 text", "description text"),
        Arrays.asList("id1"),
        Arrays.asList("id2"));
  }

  protected List<Map<String, Object>> findRows(
      String keyspaceName, String tableName, String key1, String key2) {
    String response =
        givenWithAuth()
            .queryParam("raw", "true")
            .when()
            .get(endpointTemplateForRowByPK(keyspaceName, tableName, 2), key1, key2)
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    return readJsonAs(response, LIST_OF_MAPS_TYPE);
  }
}
