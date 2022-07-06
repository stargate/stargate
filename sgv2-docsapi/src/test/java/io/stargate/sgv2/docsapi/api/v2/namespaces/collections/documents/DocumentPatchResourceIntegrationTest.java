package io.stargate.sgv2.docsapi.api.v2.namespaces.collections.documents;

import static io.restassured.RestAssured.given;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.RestAssured;
import io.stargate.sgv2.common.cql.builder.Replication;
import io.stargate.sgv2.docsapi.config.constants.Constants;
import io.stargate.sgv2.docsapi.service.schema.NamespaceManager;
import io.stargate.sgv2.docsapi.service.schema.TableManager;
import io.stargate.sgv2.docsapi.testprofiles.IntegrationTestProfile;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import javax.enterprise.context.control.ActivateRequestContext;
import javax.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.shaded.org.awaitility.Awaitility;

@QuarkusTest
@TestProfile(IntegrationTestProfile.class)
@ActivateRequestContext
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DocumentPatchResourceIntegrationTest {

  public static final String BASE_PATH = "/v2/namespaces/{namespace}/collections/{collection}";
  public static final String DEFAULT_NAMESPACE = RandomStringUtils.randomAlphanumeric(16);
  public static final String DEFAULT_COLLECTION = RandomStringUtils.randomAlphanumeric(16);
  public static final String DEFAULT_PAYLOAD =
      "{\"test\": \"document\", \"this\": [\"is\", 1, true]}";
  public static final String MALFORMED_PAYLOAD = "{\"malformed\": ";

  @Inject NamespaceManager namespaceManager;

  @Inject TableManager tableManager;

  @Inject ObjectMapper objectMapper;

  @BeforeAll
  public void init() {

    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();

    namespaceManager
        .createNamespace(DEFAULT_NAMESPACE, Replication.simpleStrategy(1))
        .await()
        .atMost(Duration.ofSeconds(10));

    tableManager
        .createCollectionTable(DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
        .await()
        .atMost(Duration.ofSeconds(10));
  }

  @Nested
  class PatchDocument {
    @Test
    public void happyPath() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .body(DEFAULT_PAYLOAD)
          .when()
          .patch(BASE_PATH + "/newdoc", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(200)
          .body("documentId", equalTo("newdoc"));

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("raw", "true")
          .when()
          .get(BASE_PATH + "/newdoc", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(200)
          .body(jsonEquals(DEFAULT_PAYLOAD));
    }

    @Test
    public void happyPathMergeExisting() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .body("{\"a\":\"b\"}")
          .when()
          .put(BASE_PATH + "/test", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(200)
          .body("documentId", equalTo("test"));

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .body(DEFAULT_PAYLOAD)
          .when()
          .patch(BASE_PATH + "/test", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(200)
          .body("documentId", equalTo("test"));

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("raw", "true")
          .when()
          .get(BASE_PATH + "/test", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(200)
          .body(jsonEquals("{\"test\": \"document\", \"this\": [\"is\", 1, true], \"a\": \"b\"}"));
    }

    @Test
    public void happyPathNoCollection() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .body(DEFAULT_PAYLOAD)
          .when()
          .patch(BASE_PATH + "/test", DEFAULT_NAMESPACE, "newtable")
          .then()
          .statusCode(200);

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("raw", "true")
          .when()
          .get(BASE_PATH + "/test", DEFAULT_NAMESPACE, "newtable")
          .then()
          .statusCode(200)
          .body(jsonEquals(DEFAULT_PAYLOAD));
    }

    @Test
    public void testRootDocumentPatch() throws IOException {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .body("{\"abc\": 1}")
          .when()
          .patch(BASE_PATH + "/1", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(200);

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("raw", "true")
          .when()
          .get(BASE_PATH + "/1", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(200)
          .body(jsonEquals("{\"abc\": 1}"));

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .body("{\"bcd\": true}")
          .when()
          .patch(BASE_PATH + "/1", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(200);

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("raw", "true")
          .when()
          .get(BASE_PATH + "/1", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(200)
          .body(jsonEquals("{\"abc\": 1, \"bcd\": true}"));

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .body("{\"bcd\": {\"a\": {\"b\": 0 }}}")
          .when()
          .patch(BASE_PATH + "/1", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(200);

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("raw", "true")
          .when()
          .get(BASE_PATH + "/1", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(200)
          .body(jsonEquals("{ \"abc\": 1, \"bcd\": {\"a\": {\"b\": 0 }} }"));

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .body("{\"bcd\": [1,2,3,4]}")
          .when()
          .patch(BASE_PATH + "/1", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(200);

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("raw", "true")
          .when()
          .get(BASE_PATH + "/1", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(200)
          .body(jsonEquals("{ \"abc\": 1, \"bcd\": [1,2,3,4] }"));

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .body("{\"bcd\": [5,{\"a\": 23},7,8]}")
          .when()
          .patch(BASE_PATH + "/1", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(200);

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("raw", "true")
          .when()
          .get(BASE_PATH + "/1", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(200)
          .body(jsonEquals("{ \"abc\": 1, \"bcd\": [5,{\"a\": 23},7,8] }"));

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .body("{\"bcd\": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]}")
          .when()
          .patch(BASE_PATH + "/1", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(200);

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("raw", "true")
          .when()
          .get(BASE_PATH + "/1", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(200)
          .body(jsonEquals("{ \"abc\": 1, \"bcd\": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12] }"));

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .body("{\"bcd\": {\"replace\": \"array\"}}")
          .when()
          .patch(BASE_PATH + "/1", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(200);

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("raw", "true")
          .when()
          .get(BASE_PATH + "/1", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(200)
          .body(jsonEquals("{ \"abc\": 1, \"bcd\": {\"replace\": \"array\"} }"));

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .body("{\"done\": \"done\"}")
          .when()
          .patch(BASE_PATH + "/1", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(200);

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("raw", "true")
          .when()
          .get(BASE_PATH + "/1", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(200)
          .body(
              jsonEquals("{ \"abc\": 1, \"bcd\": {\"replace\": \"array\"}, \"done\": \"done\" }"));
    }

    @Test
    public void withProfile() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .queryParam("profile", "true")
          .body(DEFAULT_PAYLOAD)
          .when()
          .patch(BASE_PATH + "/test", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(200)
          .body("profile", notNullValue());
    }

    @Test
    public void withTtlAutoNewDoc() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .queryParam("ttl-auto", "true")
          .body(DEFAULT_PAYLOAD)
          .when()
          .patch(BASE_PATH + "/testdoc", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(200);

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH + "/testdoc", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(200);
    }

    @Test
    public void withTtlAutoExistingDoc() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .queryParam("ttl", "5")
          .body("{\"a\":\"b\"}")
          .when()
          .put(BASE_PATH + "/test", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(200)
          .body("documentId", equalTo("test"));

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .queryParam("ttl-auto", "true")
          .body(DEFAULT_PAYLOAD)
          .when()
          .patch(BASE_PATH + "/test", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(200);

      Awaitility.await()
          .atMost(6000, TimeUnit.MILLISECONDS)
          .untilAsserted(
              () -> {
                given()
                    .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
                    .when()
                    .get(BASE_PATH + "/test", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
                    .then()
                    .statusCode(404);
              });
    }

    @Test
    public void malformedJson() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .body(MALFORMED_PAYLOAD)
          .when()
          .patch(BASE_PATH + "/test", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(400);
    }

    @Test
    public void emptyObject() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .body("{}")
          .when()
          .patch(BASE_PATH + "/test", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(400)
          .body("code", equalTo(400))
          .body(
              "description",
              equalTo("A patch operation must be done with a non-empty JSON object."));
    }

    @Test
    public void emptyArray() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .body("[]")
          .when()
          .patch(BASE_PATH + "/test", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .body("code", equalTo(400))
          .body(
              "description",
              equalTo("A patch operation must be done with a JSON object, not an array."));
    }

    @Test
    public void nonEmptyArray() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .body("[1,2,3]")
          .when()
          .patch(BASE_PATH + "/test", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .body("code", equalTo(400))
          .body(
              "description",
              equalTo("A patch operation must be done with a JSON object, not an array."));
    }

    @Test
    public void singlePrimitive() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .body("true")
          .when()
          .patch(BASE_PATH + "/test", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .body("code", equalTo(400))
          .body(
              "description",
              equalTo(
                  "Updating a key with just a JSON primitive is not allowed. Hint: update the parent path with a defined object instead."));
    }

    @Test
    public void noBody() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .when()
          .patch(BASE_PATH + "/test", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(400)
          .body("code", equalTo(400))
          .body("description", equalTo("Request invalid: must not be null."));
    }

    @Test
    public void unauthorized() {
      given()
          .header("Content-Type", "application/json")
          .body(DEFAULT_PAYLOAD)
          .when()
          .patch(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(401);
    }

    @Test
    public void keyspaceNotExists() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .body(DEFAULT_PAYLOAD)
          .when()
          .patch(BASE_PATH + "/test", "notakeyspace", DEFAULT_COLLECTION)
          .then()
          .statusCode(404)
          .body("code", equalTo(404))
          .body(
              "description", equalTo("Unknown namespace notakeyspace, you must create it first."));
    }
  }

  @Nested
  class PatchDocumentPath {

    @Test
    public void happyPath() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .body(DEFAULT_PAYLOAD)
          .when()
          .patch(BASE_PATH + "/test", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(200)
          .body("documentId", equalTo("test"));

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("raw", "true")
          .when()
          .get(BASE_PATH + "/test", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(200)
          .body(jsonEquals(DEFAULT_PAYLOAD));
    }

    @Test
    public void testPatchWithAutoTtl() throws JsonProcessingException {
      JsonNode obj1 = objectMapper.readTree("{ \"delete this\": \"in 5 seconds\" }");
      JsonNode obj2 = objectMapper.readTree("{ \"match the parent\": \"this\", \"a\": \"b\" }");

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .queryParam("ttl", "5")
          .body(obj1)
          .when()
          .put(BASE_PATH + "/test", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(200);

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .queryParam("ttl-auto", "true")
          .body(obj2)
          .when()
          .patch(BASE_PATH + "/test/1", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(200);

      Awaitility.await()
          .atMost(6000, TimeUnit.MILLISECONDS)
          .untilAsserted(
              () -> {
                // After the TTL is up, obj1 should be gone, with no remnants
                given()
                    .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
                    .when()
                    .get(BASE_PATH + "/test", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
                    .then()
                    .statusCode(404);
              });
    }

    @Test
    public void testPatchWithAutoTtlNullParent() throws JsonProcessingException {
      JsonNode obj1 =
          objectMapper.readTree("{ \"do not\": \"delete\", \"a\": {\"thing\": \"nested\"} }");
      JsonNode obj2 = objectMapper.readTree("{ \"match the parent\": \"this\", \"a\": \"b\" }");

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .body(obj1)
          .when()
          .put(BASE_PATH + "/testdoc2", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(200);

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .queryParam("ttl-auto", "true")
          .body(obj2)
          .when()
          .patch(BASE_PATH + "/testdoc2/a", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(200);

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH + "/testdoc2", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(200)
          .body(
              "data",
              jsonEquals(
                  "{ \"do not\": \"delete\", \"a\": {\"thing\": \"nested\", \"match the parent\": \"this\", \"a\": \"b\" }}"));
    }

    @Test
    public void withProfile() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .queryParam("profile", "true")
          .body(DEFAULT_PAYLOAD)
          .when()
          .patch(BASE_PATH + "/test", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(200)
          .body("profile", notNullValue());
    }

    @Test
    public void malformedJson() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .body(MALFORMED_PAYLOAD)
          .when()
          .patch(BASE_PATH + "/test", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(400);
    }

    @Test
    public void unauthorized() {
      given()
          .when()
          .patch(BASE_PATH + "/test", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(401);
    }

    @Test
    public void keyspaceNotExists() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .body(DEFAULT_PAYLOAD)
          .when()
          .patch(BASE_PATH + "/test", "notakeyspace", DEFAULT_COLLECTION)
          .then()
          .statusCode(404)
          .body("code", equalTo(404))
          .body(
              "description", equalTo("Unknown namespace notakeyspace, you must create it first."));
    }
  }
}
