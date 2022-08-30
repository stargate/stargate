package io.stargate.sgv2.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.ResourceArg;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.exception.model.dto.ApiError;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import io.stargate.sgv2.restapi.service.models.Sgv2Keyspace;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.http.HttpStatus;
import org.junit.Ignore;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestInstance;

/** Integration tests for checking CRUD schema operations for Keyspaces. */
@QuarkusIntegrationTest
@QuarkusTestResource(
    value = StargateTestResource.class,
    initArgs =
        @ResourceArg(name = StargateTestResource.Options.DISABLE_FIXED_TOKEN, value = "true"))
@TestClassOrder(ClassOrderer.DisplayName.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RestApiV2QSchemaKeyspacesIT extends RestApiV2QIntegrationTestBase {
  private static final String BASE_PATH = "/v2/schemas/keyspaces";

  // 10-Aug-2022, tatu: For our current C*/DSE Docker image, this is the one DC
  //    that is defined and can be referenced
  private static final String TEST_DC = "datacenter1";

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

  // 09-Aug-2022, tatu: Alas, Auth token seems not to be checked
  // @Ignore("Auth token handling hard-coded, won't fail as expected")
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
  // 09-Aug-2022, tatu: Datacenter in image is actually "datacenter1"
  @Test
  public void keyspaceCreateWithExplicitDC() {
    String keyspaceName = "ks_createwithdcs_" + System.currentTimeMillis();
    String requestJSON =
        String.format(
            "{\"name\": \"%s\", \"datacenters\" : [\n"
                + "       { \"name\":\"%s\", \"replicas\":1}\n"
                + "]}",
            keyspaceName, TEST_DC);
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
    expectedDCs.put(TEST_DC, new Sgv2Keyspace.Datacenter(TEST_DC, 1));
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
  // Content/response assertions
  /////////////////////////////////////////////////////////////////////////
   */

  private void assertSystemKeyspaces(Sgv2Keyspace[] keyspaces) {
    assertSimpleKeyspaces(keyspaces, "system", "system_auth", "system_schema");
  }

  private void assertSimpleKeyspaces(Sgv2Keyspace[] keyspaces, String... expectedKsNames) {
    for (String ksName : expectedKsNames) {
      assertThat(keyspaces)
          .anySatisfy(
              v -> assertThat(v).usingRecursiveComparison().isEqualTo(new Sgv2Keyspace(ksName)));
    }
  }
}
