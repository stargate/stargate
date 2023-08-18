package io.stargate.sgv2.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
import io.stargate.sgv2.common.IntegrationTestUtils;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Integration tests for accessing Vector functionality via CQL endpoint ({@code /v2/cql}) . */
@QuarkusIntegrationTest
@QuarkusTestResource(StargateTestResource.class)
@TestProfile(RestApiV2QCqlVectorIT.Profile.class)
public class RestApiV2QCqlVectorIT extends RestApiV2QIntegrationTestBase {

  // Since /cql endpoint is disabled by default, need to override
  public static class Profile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of("stargate.rest.cql.disabled", "false");
    }
  }

  public RestApiV2QCqlVectorIT() {
    super("cqlv_ks_", "cqlv_t_", KeyspaceCreation.PER_CLASS);
  }

  @BeforeAll
  public static void validateRunningOnVSearchEnabled() {
    assumeThat(IntegrationTestUtils.supportsVSearch())
        .as("Test disabled if backend does not support Vector Search (vsearch)")
        .isTrue();
  }

  private static final String CREATE_VECTOR_TABLE =
      "CREATE TABLE IF NOT EXISTS %s.%s ("
          + "id int PRIMARY KEY, embedding vector<float, 5> "
          + ")";
  private static final String CREATE_VECTOR_INDEX =
      "CREATE CUSTOM INDEX embedding_index_%s "
          + "  ON %s.%s(embedding) USING 'StorageAttachedIndex'";

  private static final String DROP_VECTOR_TABLE = "DROP TABLE IF EXISTS %s.%s";

  private static final String RESPONSE_EMPTY = "{\"count\":0,\"data\":[]}";

  /*
  /////////////////////////////////////////////////////////////////////////
  // Tests: Create with Vector, index
  /////////////////////////////////////////////////////////////////////////
   */

  // Test that we can create a table with a vector column and a vector index
  @Test
  public void tableCreateWithVectorIndex() {
    createTableEtc(testKeyspaceName(), testTableName());
  }

  // Test for basic CRUD on Vector column; no Vector Search
  @Test
  public void vectorColumnCRUD() {
    final String ks = testKeyspaceName();
    final String table = testTableName();

    createTableEtc(ks, table);

    // Insert 2 rows
    postCqlQuery(
        "INSERT into %s.%s (id, embedding) values (1, [1.0, 0.5, 0.75, 0.125, 0.25])"
            .formatted(ks, table));
    postCqlQuery(
        "INSERT into %s.%s (id, embedding) values (2, [0.5, 0.5, 0.75, 0.125, 0.25])"
            .formatted(ks, table));

    // Fetch second one back
    String resp = postCqlQuery("SELECT id, embedding from %s.%s where id = 2".formatted(ks, table));
    assertThat(resp)
        .isEqualTo("{\"count\":1,\"data\":[{\"id\":2,\"embedding\":[0.5,0.5,0.75,0.125,0.25]}]}");
  }

  // Actual Vector Search test
  @Test
  public void vectorSearch() {
    final String ks = testKeyspaceName();
    final String table = testTableName();

    createTableEtc(ks, table);

    // Insert 2 rows
    postCqlQuery(
        "INSERT into %s.%s (id, embedding) values (1, [0.25, 0.25, 0.25, 0.25, 0.125])"
            .formatted(ks, table));
    postCqlQuery(
        "INSERT into %s.%s (id, embedding) values (2, [1.0, 1.0, 1.0, 1.0, 1.0])"
            .formatted(ks, table));

    // And use Vector search to find them in order
    String resp =
        postCqlQuery(
            "SELECT id FROM %s.%s ORDER BY embedding ANN OF [1,1,1,1,1] LIMIT 10"
                .formatted(ks, table));
    assertThat(resp)
        .isEqualTo(
            "{\"count\":2,\"data\":[{\"id\":2,\"embedding\":[1.0,1.0,1.0,1.0,1.0]},"
                + "{\"id\":1,\"embedding\":[0.25,0.25,0.25,0.25,0.125]}]}");
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Helper methods
  /////////////////////////////////////////////////////////////////////////
   */

  private void createTableEtc(String ks, String tableName) {
    postAndVerifyCqlQuery(DROP_VECTOR_TABLE.formatted(ks, tableName), RESPONSE_EMPTY);
    postAndVerifyCqlQuery(CREATE_VECTOR_TABLE.formatted(ks, tableName), RESPONSE_EMPTY);
    postAndVerifyCqlQuery(CREATE_VECTOR_INDEX.formatted(tableName, ks, tableName), RESPONSE_EMPTY);
  }

  private void postAndVerifyCqlQuery(String cql, String expResponse) {
    assertThat(postCqlQuery(cql)).isEqualTo(expResponse);
  }

  private String postCqlQuery(String cql) {
    return givenWithAuth()
        .contentType(ContentType.TEXT)
        .body(cql)
        .when()
        .post(endpointPathForCQL())
        .then()
        .statusCode(200)
        .extract()
        .asString();
  }
}
