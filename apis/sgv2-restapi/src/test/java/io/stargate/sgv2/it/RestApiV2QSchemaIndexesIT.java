package io.stargate.sgv2.it;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.cql.builder.CollectionIndexingType;
import io.stargate.sgv2.api.common.exception.model.dto.ApiError;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import io.stargate.sgv2.restapi.service.models.Sgv2IndexAddRequest;
import org.apache.http.HttpStatus;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

@QuarkusIntegrationTest
@QuarkusTestResource(StargateTestResource.class)
public class RestApiV2QSchemaIndexesIT extends RestApiV2QIntegrationTestBase {
  public RestApiV2QSchemaIndexesIT() {
    super("idx_ks_", "idx_t_", KeyspaceCreation.PER_CLASS);
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

    List<IndexDesc> indexes = findAllIndexesFromSystemSchema(testKeyspaceName(), tableName);
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
    indexes = findAllIndexesFromSystemSchema(testKeyspaceName(), tableName);
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

    List<IndexDesc> indexes = findAllIndexesFromSystemSchema(testKeyspaceName(), tableName);
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
    assertThat(apiError.description()).startsWith("Column 'invalid_column' not found in table");

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
    List<IndexDesc> indexes = findAllIndexesFromSystemSchema(testKeyspaceName(), tableName);
    assertThat(indexes).isEmpty();

    Sgv2IndexAddRequest indexAdd = new Sgv2IndexAddRequest("firstName", indexName);
    indexAdd.setIfNotExists(false);
    tryCreateIndex(testKeyspaceName(), tableName, indexAdd, HttpStatus.SC_CREATED);

    // And now 1 index:
    indexes = findAllIndexesFromSystemSchema(testKeyspaceName(), tableName);
    assertThat(indexes).hasSize(1);
    assertThat(indexes.get(0).index_name).isEqualTo(indexName);

    // But then let's try DELETEing it
    String deletePath = endpointPathForIndexDelete(testKeyspaceName(), tableName, indexName);
    // Usually "no content" (returns empty String), but for fails gives ApiError
    givenWithAuth().when().delete(deletePath).then().statusCode(HttpStatus.SC_NO_CONTENT);

    // And back to "no indexes"
    indexes = findAllIndexesFromSystemSchema(testKeyspaceName(), tableName);
    assertThat(indexes).isEmpty();
  }

  @Test
  public void indexDropNoSuchTable() {
    final String tableName = "no_such_table";
    String indexName = "no_such_index";
    String deletePath = endpointPathForIndexDelete(testKeyspaceName(), tableName, indexName);

    givenWithAuth()
        .when()
        .delete(deletePath)
        .then()
        .statusCode(HttpStatus.SC_BAD_REQUEST)
        .body("code", is(HttpStatus.SC_BAD_REQUEST))
        .body("description", startsWith("Table 'no_such_table' not found"));
  }

  @Test
  public void indexDropNoSuchIndex() {
    final String tableName = testTableName();
    String indexName = "no_such_index";
    createTestTable(
        testKeyspaceName(),
        tableName,
        Arrays.asList("id text", "firstName text", "lastName text", "email list<text>"),
        Arrays.asList("id"),
        Arrays.asList());
    String deletePath = endpointPathForIndexDelete(testKeyspaceName(), tableName, indexName);
    String response =
        givenWithAuth()
            .when()
            .delete(deletePath)
            .then()
            .statusCode(HttpStatus.SC_BAD_REQUEST)
            .extract()
            .asString();
    ApiError apiError = readJsonAs(response, ApiError.class);
    assertThat(apiError.code()).isEqualTo(HttpStatus.SC_BAD_REQUEST);

    // Alas, since we get failure message from persistence backend, message varies a bit
    // (Cassandra-3.11 differs from 4.0) -- "Index 'KS.INDEX' doesn't exist" vs "Index 'INDEX' could
    // not found"
    assertThat(apiError.description()).containsPattern(".*Index '.*" + indexName + ".*");

    // But ok if defining idempotent method
    givenWithAuth()
        .queryParam("ifExists", true)
        .when()
        .delete(deletePath)
        .then()
        .statusCode(HttpStatus.SC_NO_CONTENT);
  }

  @Test
  public void indexListAllOk() {
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
    assertThat(findAllIndexesViaEndpoint(testKeyspaceName(), tableName)).hasSize(0);
    assertThat(findAllIndexesViaEndpoint(altKeyspaceName, tableName)).hasSize(0);

    final String indexName = "test_index_list_idx";
    Sgv2IndexAddRequest indexAdd = new Sgv2IndexAddRequest("firstName", indexName);
    indexAdd.setIfNotExists(false);
    tryCreateIndex(testKeyspaceName(), tableName, indexAdd, HttpStatus.SC_CREATED);

    // And now main keyspace has 1 index; the alternate one still 0
    List<IndexDesc> indexes = findAllIndexesViaEndpoint(testKeyspaceName(), tableName);
    assertThat(indexes).hasSize(1);
    assertThat(indexes.get(0).index_name).isEqualTo(indexName);

    assertThat(findAllIndexesViaEndpoint(altKeyspaceName, tableName)).hasSize(0);
  }

  /** Test to verify failure modes for "get all indexes" */
  @Test
  public void indexListAllFails() {
    // First try to access with non-existing keyspace
    givenWithAuth()
        .when()
        .get(endpointPathForAllIndexes("no-such-keyspace", "no-such-table"))
        .then()
        .statusCode(HttpStatus.SC_BAD_REQUEST)
        .body("code", Matchers.is(HttpStatus.SC_BAD_REQUEST))
        .body("description", Matchers.startsWith("Keyspace 'no-such-keyspace' not found"));

    // Then try with non-existing table
    givenWithAuth()
        .when()
        .get(endpointPathForAllIndexes(testKeyspaceName(), "no-such-table"))
        .then()
        .statusCode(HttpStatus.SC_BAD_REQUEST)
        .body("code", Matchers.is(HttpStatus.SC_BAD_REQUEST))
        .body("description", Matchers.startsWith("Table 'no-such-table' not found"));
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

  protected List<IndexDesc> findAllIndexesFromSystemSchema(String keyspaceName, String tableName) {
    List<IndexDesc> indexList = findAllIndexesFromSystemSchema();
    return indexList.stream()
        .filter(i -> keyspaceName.equals(i.keyspace_name) && tableName.equals(i.table_name))
        .collect(Collectors.toList());
  }

  protected List<IndexDesc> findAllIndexesFromSystemSchema() {
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

  record IndexDesc(
      String keyspace_name,
      String table_name,
      String index_name,
      String kind,
      Map<String, String> options) {}

  record IndexDescOptionsAsList(
          String keyspace_name,
          String table_name,
          String index_name,
          String kind,
          List<Map<String, String>> options) {}
}
