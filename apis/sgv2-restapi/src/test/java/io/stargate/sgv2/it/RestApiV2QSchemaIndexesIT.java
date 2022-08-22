package io.stargate.sgv2.it;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.api.common.cql.builder.CollectionIndexingType;
import io.stargate.sgv2.api.common.exception.model.dto.ApiError;
import io.stargate.sgv2.common.testprofiles.IntegrationTestProfile;
import io.stargate.sgv2.restapi.service.models.Sgv2IndexAddRequest;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
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
public class RestApiV2QSchemaIndexesIT extends RestApiV2QIntegrationTestBase {
  public RestApiV2QSchemaIndexesIT() {
    super("idx_ks_", "idx_t_");
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Test methods
  /////////////////////////////////////////////////////////////////////////
   */

  @Test
  public void indexCreateBasic() {
    final String tableName = testTableName();
    createTestTable(
        testKeyspaceName(),
        tableName,
        Arrays.asList("id text", "firstName text", "lastName text", "email list<text>"),
        Arrays.asList("id"),
        Arrays.asList());

    // Basically same as our helper method "createTestIndex" but write out explicitly since
    // this is test focus here
    Sgv2IndexAddRequest indexAdd = new Sgv2IndexAddRequest("firstName", "test_idx");
    indexAdd.setIfNotExists(false);

    String response =
        tryCreateIndex(testKeyspaceName(), tableName, indexAdd, HttpStatus.SC_CREATED);
    IndexResponse successResponse = readJsonAs(response, IndexResponse.class);
    assertThat(successResponse.success).isTrue();

    IndexDesc[] indexArray = findAllIndexes();
    List<IndexDesc> indexList =
        Arrays.stream(indexArray)
            .filter(
                i -> testKeyspaceName().equals(i.keyspace_name) && "test_idx".equals(i.index_name))
            .collect(Collectors.toList());
    assertThat(indexList).hasSize(1);

    // Verify that idempotent call succeeds:
    indexAdd.setIfNotExists(true);
    tryCreateIndex(testKeyspaceName(), tableName, indexAdd, HttpStatus.SC_CREATED);

    // otherwise verify failure if non-idempotent call and index already exists
    indexAdd.setIfNotExists(false);
    response = tryCreateIndex(testKeyspaceName(), tableName, indexAdd, HttpStatus.SC_BAD_REQUEST);
    ApiError apiError = readJsonAs(response, ApiError.class);
    assertThat(apiError.code()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(apiError.description()).contains("test_idx").contains("already exists");

    // successfully index a collection
    indexAdd.setColumn("email");
    indexAdd.setName(null);
    indexAdd.setKind(CollectionIndexingType.VALUES);

    response = tryCreateIndex(testKeyspaceName(), tableName, indexAdd, HttpStatus.SC_CREATED);
    successResponse = readJsonAs(response, IndexResponse.class);
    assertThat(successResponse.success).isTrue();
  }

  @Test
  public void indexCreateInvalid() {}

  @Test
  public void indexDrop() {}

  @Test
  public void indexListAll() {
    final String altKeyspaceName = "alt_" + System.currentTimeMillis();
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Helper methods
  /////////////////////////////////////////////////////////////////////////
  */

  protected IndexDesc[] findAllIndexes() {
    final String path = endpointPathForAllRows("system_schema", "indexes");
    String response =
        given()
            .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
            .queryParam("raw", true)
            .queryParam("fields", "keyspace_name,index_name,table_name,kind")
            .when()
            .get(path)
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    return readJsonAs(response, IndexDesc[].class);
  }

  protected String tryCreateIndex(
      String keyspaceName, String tableName, Sgv2IndexAddRequest indexAdd, int expectedResult) {
    return given()
        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
        .contentType(ContentType.JSON)
        .body(asJsonString(indexAdd))
        .when()
        .post(endpointPathForIndexAdd(keyspaceName, tableName))
        .then()
        .statusCode(expectedResult)
        .extract()
        .asString();
  }

  static class IndexDesc {
    public String keyspace_name;
    public String table_name;
    public String index_name;
    public String kind;
  }
}
