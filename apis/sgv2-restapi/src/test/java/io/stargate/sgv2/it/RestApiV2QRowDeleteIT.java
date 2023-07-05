package io.stargate.sgv2.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.api.common.exception.model.dto.ApiError;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@QuarkusTestResource(StargateTestResource.class)
public class RestApiV2QRowDeleteIT extends RestApiV2QIntegrationTestBase {
  public RestApiV2QRowDeleteIT() {
    super("rowdel_ks_", "rowdel_t_", KeyspaceCreation.PER_CLASS);
  }

  @Test
  public void deleteRow() {
    final String tableName = testTableName();
    createSimpleTestTable(testKeyspaceName(), tableName);

    final String rowIdentifier = UUID.randomUUID().toString();
    Map<String, String> row = new HashMap<>();
    row.put("id", rowIdentifier);
    row.put("firstName", "John");
    insertRow(testKeyspaceName(), tableName, row);

    assertThat(findRowsAsList(testKeyspaceName(), tableName, rowIdentifier)).hasSize(1);

    // Successful deletion
    deleteRow(endpointPathForRowByPK(testKeyspaceName(), tableName, rowIdentifier));
    assertThat(findRowsAsList(testKeyspaceName(), tableName, rowIdentifier)).hasSize(0);
  }

  @Test
  public void deleteRowClustering() {
    final String tableName = testTableName();
    final Integer rowIdentifier = setupClusteringTestCase(testKeyspaceName(), tableName);

    // Validate contents before operation, but just once
    List<Map<String, Object>> rows = findRowsAsList(testKeyspaceName(), tableName, rowIdentifier);
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0).get("id")).isEqualTo(1);
    assertThat(rows.get(0).get("expense_id")).isEqualTo(1);
    assertThat(rows.get(1).get("id")).isEqualTo(1);
    assertThat(rows.get(1).get("expense_id")).isEqualTo(2);

    // And delete the first expense:
    deleteRow(endpointPathForRowByPK(testKeyspaceName(), tableName, rowIdentifier, 1));
    // then verify only second one remains
    rows = findRowsAsList(testKeyspaceName(), tableName, rowIdentifier);
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).get("id")).isEqualTo(1);
    assertThat(rows.get(0).get("expense_id")).isEqualTo(2);
  }

  @Test
  public void deleteRowByPartitionKey() {
    final String tableName = testTableName();
    setupClusteringTestCase(testKeyspaceName(), tableName);

    // Just validate counts before deletion(s)
    assertThat(findRowsAsList(testKeyspaceName(), tableName, 1)).hasSize(2);
    assertThat(findRowsAsList(testKeyspaceName(), tableName, 2)).hasSize(1);

    // Try deleting both expenses of the first
    deleteRow(endpointPathForRowByPK(testKeyspaceName(), tableName, 1));
    assertThat(findRowsAsList(testKeyspaceName(), tableName, 1)).hasSize(0);

    // and verify we still have the other expenses
    List<Map<String, Object>> rows = findRowsAsList(testKeyspaceName(), tableName, "2");
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).get("id")).isEqualTo(2);
    assertThat(rows.get(0).get("expense_id")).isEqualTo(1);
    assertThat(rows.get(0).get("firstName")).isEqualTo("Jane");
  }

  @Test
  public void deleteRowsWithMixedClustering() {
    final String tableName = testTableName();
    setupMixedClusteringTestCase(testKeyspaceName(), tableName);

    List<Map<String, Object>> rows = findRowsAsList(testKeyspaceName(), tableName, 1, "one", -1);
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0).get("v")).isEqualTo(9);
    assertThat(rows.get(1).get("v")).isEqualTo(19);

    deleteRow(endpointPathForRowByPK(testKeyspaceName(), tableName, 1, "one", -1));

    rows = findRowsAsList(testKeyspaceName(), tableName, 1, "one", -1);
    assertThat(rows).hasSize(0);
  }

  @Test
  public void deleteRowsMixedClusteringAndCK() {
    final String tableName = testTableName();
    setupMixedClusteringTestCase(testKeyspaceName(), tableName);

    List<Map<String, Object>> rows = findRowsAsList(testKeyspaceName(), tableName, 1, "one", -1);
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0).get("ck0")).isEqualTo(10);
    assertThat(rows.get(0).get("ck1")).isEqualTo("foo");
    assertThat(rows.get(0).get("v")).isEqualTo(9);
    assertThat(rows.get(1).get("ck0")).isEqualTo(20);
    assertThat(rows.get(1).get("ck1")).isEqualTo("foo");
    assertThat(rows.get(1).get("v")).isEqualTo(19);

    deleteRow(endpointPathForRowByPK(testKeyspaceName(), tableName, 1, "one", -1, 20));

    rows = findRowsAsList(testKeyspaceName(), tableName, 1, "one", -1, 20);
    assertThat(rows).hasSize(0);

    rows = findRowsAsList(testKeyspaceName(), tableName, 1, "one", -1);
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).get("v")).isEqualTo(9);
  }

  @Test
  public void deleteRowNoSuchKey() {
    final String tableName = testTableName();
    createSimpleTestTable(testKeyspaceName(), tableName);

    final String rowIdentifier = UUID.randomUUID().toString();

    // To keep DELETE idempotent, it always succeeds even if no rows match, so:
    deleteRow(
        endpointPathForRowByPK(testKeyspaceName(), tableName, rowIdentifier),
        HttpStatus.SC_NO_CONTENT);

    // But with invalid key (not UUID) we should fail
    String response =
        deleteRow(
            endpointPathForRowByPK(testKeyspaceName(), tableName, "not-really-uuid"),
            HttpStatus.SC_BAD_REQUEST);
    ApiError error = readJsonAs(response, ApiError.class);
    assertThat(error.code()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(error.description())
        .contains("Invalid path for row to delete, problem")
        .contains("Invalid String value")
        .contains("'not-really-uuid'");
  }
}
