package io.stargate.sgv2.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.ResourceArg;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.exception.model.dto.ApiError;
import io.stargate.sgv2.common.IntegrationTestUtils;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import io.stargate.sgv2.restapi.service.models.Sgv2Keyspace;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.http.HttpStatus;
import org.junit.Ignore;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration tests for checking CRUD schema operations for Keyspaces. */
@QuarkusIntegrationTest
@QuarkusTestResource(
    value = StargateTestResource.class,
    initArgs =
        @ResourceArg(name = StargateTestResource.Options.DISABLE_FIXED_TOKEN, value = "true"))
public class RestApiV2QSchemaKeyspacesIT extends RestApiV2QIntegrationTestBase {
  private static final String BASE_PATH = "/v2/schemas/keyspaces";

  private static final Logger LOG = LoggerFactory.getLogger(RestApiV2QSchemaKeyspacesIT.class);

  public RestApiV2QSchemaKeyspacesIT() {
    super("ks_ks_", "ks_t_");
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Tests: GET
  /////////////////////////////////////////////////////////////////////////
   */

  @Test
  public void keyspacesGetAll() {
    String response =
        givenWithAuth()
            .when()
            .get(BASE_PATH)
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    assertSystemKeyspaces(readWrappedRESTResponse(response, Sgv2Keyspace[].class));
  }

  @Test
  public void keyspacesGetAllRaw() {
    String response =
        givenWithAuth()
            .queryParam("raw", "true")
            .when()
            .get(BASE_PATH)
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    assertSystemKeyspaces(readJsonAs(response, Sgv2Keyspace[].class));
  }

  @Test
  public void keyspacesGetAllMissingToken() {
    String response =
        givenWithoutAuth()
            .when()
            .get(BASE_PATH)
            .then()
            .statusCode(HttpStatus.SC_UNAUTHORIZED)
            .extract()
            .asString();
  }

  // 09-Aug-2022, tatu: Alas, Auth token seems not to be checked
  @Ignore("Auth token handling hard-coded, won't fail as expected")
  public void keyspacesGetAllBadToken() {
    String response =
        givenWithAuthToken("NotAPassword")
            .when()
            .get(BASE_PATH)
            .then()
            .statusCode(HttpStatus.SC_UNAUTHORIZED)
            .extract()
            .asString();
  }

  @Test
  public void keyspaceGetWrapped() {
    String response =
        givenWithAuth()
            .when()
            .get(BASE_PATH + "/system")
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    final Sgv2Keyspace keyspace = readWrappedRESTResponse(response, Sgv2Keyspace.class);
    assertThat(keyspace).usingRecursiveComparison().isEqualTo(new Sgv2Keyspace("system"));
  }

  @Test
  public void keyspaceGetRaw() {
    String response =
        givenWithAuth()
            .queryParam("raw", "true")
            .when()
            .get(BASE_PATH + "/system")
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    final Sgv2Keyspace keyspace = readJsonAs(response, Sgv2Keyspace.class);
    assertThat(keyspace).usingRecursiveComparison().isEqualTo(new Sgv2Keyspace("system"));
  }

  @Test
  public void keyspaceGetNotFound() {
    String response =
        givenWithAuth()
            .when()
            .get(BASE_PATH + "/ks_not_found")
            .then()
            .statusCode(HttpStatus.SC_NOT_FOUND)
            .extract()
            .asString();
    final ApiError error = readJsonAs(response, ApiError.class);
    assertThat(error.code()).isEqualTo(HttpStatus.SC_NOT_FOUND);
    assertThat(error.description()).isEqualTo("Unable to describe keyspace 'ks_not_found'");
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Tests: Create
  /////////////////////////////////////////////////////////////////////////
   */

  @Test
  public void keyspaceCreateSimple() {
    // NOTE: we may have created a keyspace already but don't use that, instead:
    String keyspaceName = "ks_create_" + System.currentTimeMillis();
    createKeyspace(keyspaceName);

    String response =
        givenWithAuth()
            .queryParam("raw", "true")
            .when()
            .get(BASE_PATH + "/{keyspace-id}", keyspaceName)
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    final Sgv2Keyspace keyspace = readJsonAs(response, Sgv2Keyspace.class);
    assertThat(keyspace).usingRecursiveComparison().isEqualTo(new Sgv2Keyspace(keyspaceName));
  }

  // For [stargate#1817]. Unfortunately our current CI set up only has single DC ("dc1")
  // configured, with a single node. But this is only about constructing keyspace anyway,
  // including handling of variant request structure; as long as that maps to query builder
  // we should be good.
  // 01-Sep-2022, tatu: Datacenter apparently varies between C*3/4 and DSE; either "dc1"
  //    or "datacenter1". So need to detect dynamically for more robust testing.
  @Test
  public void keyspaceCreateWithExplicitDC() {
    final String testDC = selectPrimaryDC();
    String keyspaceName = "ks_createwithdcs_" + System.currentTimeMillis();
    String requestJSON =
        String.format(
            "{\"name\": \"%s\", \"datacenters\" : [\n"
                + "       { \"name\":\"%s\", \"replicas\":1}\n"
                + "]}",
            keyspaceName, testDC);
    givenWithAuth()
        .contentType(ContentType.JSON)
        .body(requestJSON)
        .when()
        .post(BASE_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);

    // Then validate it was created as expected
    String response =
        givenWithAuth()
            .queryParam("raw", "true")
            .when()
            .get(BASE_PATH + "/{keyspace-id}", keyspaceName)
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    final Sgv2Keyspace keyspace = readJsonAs(response, Sgv2Keyspace.class);

    assertThat(keyspace.getName()).isEqualTo(keyspaceName);

    Map<String, Sgv2Keyspace.Datacenter> expectedDCs = new HashMap<>();
    expectedDCs.put(testDC, new Sgv2Keyspace.Datacenter(testDC, 1));
    Optional<List<Sgv2Keyspace.Datacenter>> dcs = Optional.ofNullable(keyspace.getDatacenters());
    Map<String, Sgv2Keyspace.Datacenter> actualDCs =
        dcs.orElse(Collections.emptyList()).stream()
            .collect(Collectors.toMap(Sgv2Keyspace.Datacenter::getName, Function.identity()));
    assertThat(actualDCs).usingRecursiveComparison().isEqualTo(expectedDCs);
  }

  @Test
  public void keyspaceCreateWithInvalidJson() {
    String response =
        givenWithAuth()
            .contentType(ContentType.JSON)
            // non-JSON, missing colon after "name":
            .body("{\"name\" \"badjsonkeyspace\", \"replicas\": 1}")
            .when()
            .post(BASE_PATH)
            .then()
            .statusCode(HttpStatus.SC_BAD_REQUEST)
            .extract()
            .asString();

    final ApiError error = readJsonAs(response, ApiError.class);
    assertThat(error.code()).isEqualTo(HttpStatus.SC_BAD_REQUEST);
    assertThat(error.description())
        .startsWith("Invalid JSON payload for Keyspace creation, problem: ");
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Tests: Delete
  /////////////////////////////////////////////////////////////////////////
   */

  @Test
  public void keyspaceDelete() {
    String keyspaceName = "ks_delete_" + System.currentTimeMillis();
    createKeyspace(keyspaceName);

    // Verify it was created:
    givenWithAuth()
        .when()
        .get(BASE_PATH + "/{keyspace-id}", keyspaceName)
        .then()
        .statusCode(HttpStatus.SC_OK);

    // Then delete

    givenWithAuth()
        .when()
        .delete(BASE_PATH + "/{keyspace-id}", keyspaceName)
        .then()
        .statusCode(HttpStatus.SC_NO_CONTENT);

    // And finally verify it's gone
    givenWithAuth()
        .when()
        .get(BASE_PATH + "/{keyspace-id}", keyspaceName)
        .then()
        .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Helper methods, metadata access
  /////////////////////////////////////////////////////////////////////////
   */

  protected String selectPrimaryDC() {
    String clusterVersion = IntegrationTestUtils.getClusterVersion();
    final String dc;

    switch (clusterVersion) {
      case "6.8": // DSE has different one
        dc = "dc1";
        break;
      default:
        dc = "datacenter1";
    }
    LOG.info(
        "selectPrimaryDC() selects '{}' as the default DC given cluster version of '{}'",
        dc,
        clusterVersion);
    return dc;
  }

  protected String findPrimaryDC() {
    final String path = endpointPathForAllRows("system", "local");
    String response =
        givenWithAuth()
            .queryParam("raw", "true")
            .when()
            .get(path)
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    JsonNode entries = readJsonAsTree(response);
    String dc = entries.at("/0/data_center").asText();
    LOG.info(
        "findPrimaryDC() found {} entries: detected '{}' as the primary DC", entries.size(), dc);
    assertThat(entries).hasSizeGreaterThan(0);
    assertThat(dc).isNotEmpty();
    return dc;
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Helper methods, assertions
  /////////////////////////////////////////////////////////////////////////
   */

  private void assertSystemKeyspaces(Sgv2Keyspace[] keyspaces) {
    // While C*3, C*4, DSE have "system", "system_auth", "system_schema", CNDB
    // does not have "system_auth"
    assertSimpleKeyspaces(keyspaces, "system", "system_schema");
  }

  private void assertSimpleKeyspaces(Sgv2Keyspace[] keyspaces, String... expectedKsNames) {
    Set<String> existingNames =
        Arrays.stream(keyspaces)
            .map(k -> k.getName())
            .collect(Collectors.toCollection(TreeSet::new));
    for (String expectedName : expectedKsNames) {
      assertThat(existingNames).contains(expectedName);
    }
  }
}
