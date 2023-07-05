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

  private static final String TEST_KEY1 = "key11/ab&cd/xy";
  private static final String TEST_KEY2 = "key2/ef&gh/yz";

  @Test
  public void getWithSpecialCharsInPK() {
    final String tableName = testTableName();
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
        Map.of("id1", TEST_KEY1, "id2", TEST_KEY2, "description", "Desc"));

    String response =
        givenWithAuth()
            .queryParam("raw", "false")
            .when()
            .get(endpointTemplateForRowByPK(testKeyspaceName(), tableName, 2), TEST_KEY1, TEST_KEY2)
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
    assertThat(data.get(0).get("id1")).isEqualTo(TEST_KEY1);
    assertThat(data.get(0).get("id2")).isEqualTo(TEST_KEY2);
    assertThat(data.get(0).get("description")).isEqualTo("Desc");
  }

  @Test
  public void updateWithSpecialCharsInPK() {
    final String tableName = testTableName();
    createPKTestTable(tableName);

    insertRow(
        testKeyspaceName(),
        tableName,
        Map.of("id1", TEST_KEY1, "id2", TEST_KEY2, "description", "Desc"));

    Map<String, String> rowUpdate = Map.of("description", "Updated Desc");
    final String payloadJSON = asJsonString(rowUpdate);
    givenWithAuth()
        .queryParam("raw", false)
        .contentType(ContentType.JSON)
        .body(payloadJSON)
        .when()
        .put(endpointTemplateForRowByPK(testKeyspaceName(), tableName, 2), TEST_KEY1, TEST_KEY2)
        .then()
        .statusCode(HttpStatus.SC_OK);

    List<Map<String, Object>> rows =
        findRowsAsList(testKeyspaceName(), tableName, TEST_KEY1, TEST_KEY2);
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).get("id1")).isEqualTo(TEST_KEY1);
    assertThat(rows.get(0).get("id2")).isEqualTo(TEST_KEY2);
    assertThat(rows.get(0).get("description")).isEqualTo("Updated Desc");
  }

  @Test
  public void patchWithSpecialCharsInPK() {
    final String tableName = testTableName();
    createPKTestTable(tableName);

    insertRow(
        testKeyspaceName(),
        tableName,
        Map.of("id1", TEST_KEY1, "id2", TEST_KEY2, "description", "Desc"));

    Map<String, String> rowUpdate = Map.of("description", "Patched Desc");
    final String payloadJSON = asJsonString(rowUpdate);
    givenWithAuth()
        .queryParam("raw", true)
        .contentType(ContentType.JSON)
        .body(payloadJSON)
        .when()
        .patch(endpointTemplateForRowByPK(testKeyspaceName(), tableName, 2), TEST_KEY1, TEST_KEY2)
        .then()
        .statusCode(HttpStatus.SC_OK);

    List<Map<String, Object>> rows =
        findRowsAsList(testKeyspaceName(), tableName, TEST_KEY1, TEST_KEY2);
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).get("id1")).isEqualTo(TEST_KEY1);
    assertThat(rows.get(0).get("id2")).isEqualTo(TEST_KEY2);
    assertThat(rows.get(0).get("description")).isEqualTo("Patched Desc");
  }

  @Test
  public void deleteWithSpecialCharsInPK() {
    final String tableName = testTableName();
    createPKTestTable(tableName);

    insertRow(
        testKeyspaceName(),
        tableName,
        Map.of("id1", TEST_KEY1, "id2", TEST_KEY2, "description", "Desc"));

    assertThat(findRowsAsList(testKeyspaceName(), tableName, TEST_KEY1, TEST_KEY2)).hasSize(1);
    givenWithAuth()
        .when()
        .delete(endpointTemplateForRowByPK(testKeyspaceName(), tableName, 2), TEST_KEY1, TEST_KEY2)
        .then()
        .statusCode(HttpStatus.SC_NO_CONTENT);
    assertThat(findRowsAsList(testKeyspaceName(), tableName, TEST_KEY1, TEST_KEY2)).hasSize(0);
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
}
