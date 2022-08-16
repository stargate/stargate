package io.stargate.sgv2.it;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.api.common.exception.model.dto.ApiError;
import io.stargate.sgv2.common.testprofiles.IntegrationTestProfile;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.enterprise.context.control.ActivateRequestContext;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestInstance;

@QuarkusTest
@TestProfile(IntegrationTestProfile.class)
@ActivateRequestContext
@TestClassOrder(ClassOrderer.DisplayName.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RestApiV2QRowDeleteIT extends RestApiV2QIntegrationTestBase {
  public RestApiV2QRowDeleteIT() {
    super("rowdel_ks_", "rowdel_t_");
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

    // Successful deletion
    deleteRow(endpointPathForRowByPK(testKeyspaceName(), tableName, rowIdentifier));
  }

  @Test
  public void deleteRowByPartitionKey() {}

  @Test
  public void deleteRowClustering() {}

  @Test
  public void deleteRowsMixedClusteringAndCK() {}

  @Test
  public void deleteRowsWithMixedClustering() {}

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

  /*
  /////////////////////////////////////////////////////////////////////////
  // Helper methods
  /////////////////////////////////////////////////////////////////////////
   */

  private String deleteRow(String deletePath) {
    return deleteRow(deletePath, HttpStatus.SC_NO_CONTENT);
  }

  private String deleteRow(String deletePath, int expectedStatus) {
    // Usually "no content" (returns empty String), but for fails gives ApiError
    return given()
        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
        .when()
        .delete(deletePath)
        .then()
        .statusCode(expectedStatus)
        .extract()
        .asString();
  }
}
