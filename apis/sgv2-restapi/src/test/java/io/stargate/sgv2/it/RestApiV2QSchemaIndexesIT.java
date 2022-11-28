package io.stargate.sgv2.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.cql.builder.CollectionIndexingType;
import io.stargate.sgv2.api.common.exception.model.dto.ApiError;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import io.stargate.sgv2.restapi.service.models.Sgv2IndexAddRequest;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@QuarkusTestResource(StargateTestResource.class)
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

    List<IndexDesc> indexes = findAllIndexes(testKeyspaceName(), tableName);
    assertThat(indexes).hasSize(1);

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
    indexAdd.setName("email_idx");
    indexAdd.setKind(CollectionIndexingType.VALUES);

    response = tryCreateIndex(testKeyspaceName(), tableName, indexAdd, HttpStatus.SC_CREATED);
    successResponse = readJsonAs(response, IndexResponse.class);
    assertThat(successResponse.success).isTrue();

    // and verify we can now see 2 indexes
    indexes = findAllIndexes(testKeyspaceName(), tableName);
    assertThat(indexes).hasSize(2);
  }

  // For https://github.com/stargate/stargate/issues/2244
  @Test
  public void indexCreateIssue2244() {
    final String tableName = testTableName();
    createTestTable(
        testKeyspaceName(),
        tableName,
        Arrays.asList(
            "genre text",
            "year int",
            "title text",
            "formats frozen<map<text,text>>",
            "tuples frozen<tuple<text,text,text>>",
            "upload timestamp",
            "frames list<int>",
            "tags set<text>"),
        Arrays.asList("genre", "year", "title"),
        Arrays.asList("year", "title"));

    Sgv2IndexAddRequest indexAdd = new Sgv2IndexAddRequest("title", "test_idx_2244");
    indexAdd.setIfNotExists(false);

    String response =
        tryCreateIndex(testKeyspaceName(), tableName, indexAdd, HttpStatus.SC_CREATED);
    IndexResponse successResponse = readJsonAs(response, IndexResponse.class);
    assertThat(successResponse.success).isTrue();

    List<IndexDesc> indexes = findAllIndexes(testKeyspaceName(), tableName);
    assertThat(indexes).hasSize(1);
  }

  @Test
  public void indexCreateInvalid() {
    final String tableName = testTableName();
    createTestTable(
        testKeyspaceName(),
        tableName,
        Arrays.asList("id text", "firstName text", "lastName text", "email list<text>"),
        Arrays.asList("id"),
        Arrays.asList());

    // Invalid table
    final String invalidTable = "no_such_table";
    Sgv2IndexAddRequest indexAdd = new Sgv2IndexAddRequest("firstName", "invalid_idx");
    indexAdd.setIfNotExists(true);
    String response =
        tryCreateIndex(testKeyspaceName(), invalidTable, indexAdd, HttpStatus.SC_BAD_REQUEST);
    ApiError apiError = readJsonAs(response, ApiError.class);
    assertThat(apiError.code()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(apiError.description()).containsPattern("Table.*not found");

    // Invalid column. For some reason 404 whereas for table we get 400. Not sure why
    // but at this point it is done for backwards compatibility
    indexAdd.setColumn("invalid_column");
    response = tryCreateIndex(testKeyspaceName(), tableName, indexAdd, HttpStatus.SC_NOT_FOUND);
    apiError = readJsonAs(response, ApiError.class);
    assertThat(apiError.code()).isEqualTo(HttpStatus.SC_NOT_FOUND);
    assertThat(apiError.description()).isEqualTo("Column 'invalid_column' not found in table.");

    // invalid index kind
    indexAdd.setColumn("firstName");
    indexAdd.setKind(CollectionIndexingType.ENTRIES);
    response = tryCreateIndex(testKeyspaceName(), tableName, indexAdd, HttpStatus.SC_BAD_REQUEST);
    apiError = readJsonAs(response, ApiError.class);
    assertThat(apiError.code()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(apiError.description()).contains("Cannot create entries() index on firstName");
  }

  @Test
  public void indexDrop() {
    final String tableName = testTableName();
    String indexName = "test_idx_to_drop";
    createTestTable(
        testKeyspaceName(),
        tableName,
        Arrays.asList("id text", "firstName text", "lastName text", "email list<text>"),
        Arrays.asList("id"),
        Arrays.asList());
    // No indexes, initially
    List<IndexDesc> indexes = findAllIndexes(testKeyspaceName(), tableName);
    assertThat(indexes).isEmpty();

    Sgv2IndexAddRequest indexAdd = new Sgv2IndexAddRequest("firstName", indexName);
    indexAdd.setIfNotExists(false);
    tryCreateIndex(testKeyspaceName(), tableName, indexAdd, HttpStatus.SC_CREATED);

    // And now 1 index:
    indexes = findAllIndexes(testKeyspaceName(), tableName);
    assertThat(indexes).hasSize(1);
    assertThat(indexes.get(0).index_name).isEqualTo(indexName);

    // But then let's try DELETEing it
    String deletePath = endpointPathForIndexDelete(testKeyspaceName(), tableName, indexName);
    // Usually "no content" (returns empty String), but for fails gives ApiError
    givenWithAuth().when().delete(deletePath).then().statusCode(HttpStatus.SC_NO_CONTENT);

    // And back to "no indexes"
    indexes = findAllIndexes(testKeyspaceName(), tableName);
    assertThat(indexes).isEmpty();

    // And now an invalid case (could extract into separate test method in future
    indexName = "no_such_index";
    deletePath = endpointPathForIndexDelete(testKeyspaceName(), tableName, indexName);
    String response =
        givenWithAuth()
            .when()
            .delete(deletePath)
            .then()
            .statusCode(HttpStatus.SC_NOT_FOUND)
            .extract()
            .asString();
    ApiError apiError = readJsonAs(response, ApiError.class);
    assertThat(apiError.code()).isEqualTo(HttpStatus.SC_NOT_FOUND);
    assertThat(apiError.description()).isEqualTo("Index '" + indexName + "' not found.");

    // But ok if defining idempotent method
    givenWithAuth()
        .queryParam("ifExists", true)
        .when()
        .delete(deletePath)
        .then()
        .statusCode(HttpStatus.SC_NO_CONTENT);
  }

  @Test
  public void indexListAll() {
    final String tableName = testTableName();
    final String altKeyspaceName = "idx_ks_indexListAlternate" + System.currentTimeMillis();
    createKeyspace(altKeyspaceName);
    createTestTable(
        testKeyspaceName(),
        tableName,
        Arrays.asList("id text", "firstName text", "email list<text>"),
        Arrays.asList("id"),
        Arrays.asList());
    // We'll need another table in another keyspace to test [stargate#1463]
    createTestTable(
        altKeyspaceName,
        tableName,
        Arrays.asList("id text", "firstName text", "email list<text>"),
        Arrays.asList("id"),
        Arrays.asList());

    // Verify that neither keyspace (primary test; alt) have no indexes defined:
    assertThat(findAllIndexes(testKeyspaceName(), tableName)).hasSize(0);
    assertThat(findAllIndexes(altKeyspaceName, tableName)).hasSize(0);

    final String indexName = "test_index_list_idx";
    Sgv2IndexAddRequest indexAdd = new Sgv2IndexAddRequest("firstName", indexName);
    indexAdd.setIfNotExists(false);
    tryCreateIndex(testKeyspaceName(), tableName, indexAdd, HttpStatus.SC_CREATED);

    // And now main keyspace has 1 index; the alternate one still 0
    List<IndexDesc> indexes = findAllIndexes(testKeyspaceName(), tableName);
    assertThat(indexes).hasSize(1);
    assertThat(indexes.get(0).index_name).isEqualTo(indexName);

    assertThat(findAllIndexes(altKeyspaceName, tableName)).hasSize(0);
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Helper methods
  /////////////////////////////////////////////////////////////////////////
  */

  protected String endpointPathForIndexDelete(String ksName, String tableName, String indexName) {
    return String.format(
        "/v2/schemas/keyspaces/%s/tables/%s/indexes/%s", ksName, tableName, indexName);
  }

  protected List<IndexDesc> findAllIndexes() {
    final String path = endpointPathForAllRows("system_schema", "indexes");
    String response =
        givenWithAuth()
            .queryParam("raw", true)
            .queryParam("fields", "keyspace_name,index_name,table_name,kind")
            .when()
            .get(path)
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    return Arrays.asList(readJsonAs(response, IndexDesc[].class));
  }

  protected List<IndexDesc> findAllIndexes(String keyspaceName, String tableName) {
    List<IndexDesc> indexList = findAllIndexes();
    return indexList.stream()
        .filter(i -> keyspaceName.equals(i.keyspace_name) && tableName.equals(i.table_name))
        .collect(Collectors.toList());
  }

  protected String tryCreateIndex(
      String keyspaceName, String tableName, Sgv2IndexAddRequest indexAdd, int expectedResult) {
    return givenWithAuth()
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

    @Override
    public String toString() {
      return String.format(
          "[IndexDesc: keyspace=%s, table=%s, name=%s, kind=%s]",
          keyspace_name, table_name, index_name, kind);
    }
  }
}
