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
import io.restassured.http.ContentType;
import io.stargate.sgv2.common.cql.builder.Replication;
import io.stargate.sgv2.docsapi.config.constants.HttpConstants;
import io.stargate.sgv2.docsapi.service.schema.NamespaceManager;
import io.stargate.sgv2.docsapi.service.schema.TableManager;
import io.stargate.sgv2.docsapi.testprofiles.IntegrationTestProfile;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import javax.enterprise.context.control.ActivateRequestContext;
import javax.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.shaded.org.awaitility.Awaitility;

@QuarkusTest
@TestProfile(IntegrationTestProfile.class)
@ActivateRequestContext
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DocumentPatchResourceIntegrationTest {

  public static final String BASE_PATH =
      "/v2/namespaces/{namespace}/collections/{collection}/{document-id}";
  public static final String DEFAULT_NAMESPACE = RandomStringUtils.randomAlphanumeric(16);
  public static final String DEFAULT_COLLECTION = RandomStringUtils.randomAlphanumeric(16);
  public String documentId;
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

  @BeforeEach
  public void setup() {
    documentId = RandomStringUtils.randomAlphanumeric(16);
  }

  @Nested
  class PatchDocument {
    @Test
    public void happyPath() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(DEFAULT_PAYLOAD)
          .when()
          .patch(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200)
          .body("documentId", equalTo(documentId));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("raw", "true")
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200)
          .body(jsonEquals(DEFAULT_PAYLOAD));
    }

    @Test
    public void happyPathMergeExisting() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body("{\"a\":\"b\"}")
          .when()
          .put(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200)
          .body("documentId", equalTo(documentId));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(DEFAULT_PAYLOAD)
          .when()
          .patch(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200)
          .body("documentId", equalTo(documentId));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("raw", "true")
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200)
          .body(jsonEquals("{\"test\": \"document\", \"this\": [\"is\", 1, true], \"a\": \"b\"}"));
    }

    @Test
    public void happyPathNoCollection() {
      String tableName = RandomStringUtils.randomAlphanumeric(16);
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(DEFAULT_PAYLOAD)
          .when()
          .patch(BASE_PATH, DEFAULT_NAMESPACE, tableName, documentId)
          .then()
          .statusCode(200);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("raw", "true")
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, tableName, documentId)
          .then()
          .statusCode(200)
          .body(jsonEquals(DEFAULT_PAYLOAD));
    }

    @Test
    public void rootDocumentPatch() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body("{\"abc\": 1}")
          .when()
          .patch(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("raw", "true")
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200)
          .body(jsonEquals("{\"abc\": 1}"));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body("{\"bcd\": true}")
          .when()
          .patch(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("raw", "true")
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200)
          .body(jsonEquals("{\"abc\": 1, \"bcd\": true}"));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body("{\"bcd\": {\"a\": {\"b\": 0 }}}")
          .when()
          .patch(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("raw", "true")
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200)
          .body(jsonEquals("{ \"abc\": 1, \"bcd\": {\"a\": {\"b\": 0 }} }"));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body("{\"bcd\": [1,2,3,4]}")
          .when()
          .patch(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("raw", "true")
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200)
          .body(jsonEquals("{ \"abc\": 1, \"bcd\": [1,2,3,4] }"));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body("{\"bcd\": [5,{\"a\": 23},7,8]}")
          .when()
          .patch(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("raw", "true")
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200)
          .body(jsonEquals("{ \"abc\": 1, \"bcd\": [5,{\"a\": 23},7,8] }"));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body("{\"bcd\": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]}")
          .when()
          .patch(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("raw", "true")
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200)
          .body(jsonEquals("{ \"abc\": 1, \"bcd\": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12] }"));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body("{\"bcd\": {\"replace\": \"array\"}}")
          .when()
          .patch(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("raw", "true")
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200)
          .body(jsonEquals("{ \"abc\": 1, \"bcd\": {\"replace\": \"array\"} }"));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body("{\"done\": \"done\"}")
          .when()
          .patch(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("raw", "true")
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200)
          .body(
              jsonEquals("{ \"abc\": 1, \"bcd\": {\"replace\": \"array\"}, \"done\": \"done\" }"));
    }

    @Test
    public void rootDocumentPatchNulls() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body("{\"abc\": null}")
          .when()
          .patch(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("raw", "true")
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200)
          .body(jsonEquals("{\"abc\": null}"));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body("{\"bcd\": null}")
          .when()
          .patch(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("raw", "true")
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200)
          .body(jsonEquals("{\"abc\": null, \"bcd\": null}"));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body("{\"bcd\": {\"a\": {\"b\": null }}}")
          .when()
          .patch(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("raw", "true")
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200)
          .body(jsonEquals("{ \"abc\": null, \"bcd\": {\"a\": {\"b\": null }} }"));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body("{\"bcd\": [null,null,null,null]}")
          .when()
          .patch(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("raw", "true")
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200)
          .body(jsonEquals("{ \"abc\": null, \"bcd\": [null,null,null,null] }"));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body("{\"bcd\": [null,{\"a\": null},null,null]}")
          .when()
          .patch(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("raw", "true")
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200)
          .body(jsonEquals("{ \"abc\": null, \"bcd\": [null,{\"a\": null},null,null] }"));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(
              "{\"bcd\": [null, null, null, null, null, null, null, null, null, null, null, null, null]}")
          .when()
          .patch(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("raw", "true")
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200)
          .body(
              jsonEquals(
                  "{ \"abc\": null, \"bcd\": [null, null, null, null, null, null, null, null, null, null, null, null, null] }"));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body("{\"bcd\": {\"replace\": null}}")
          .when()
          .patch(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("raw", "true")
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200)
          .body(jsonEquals("{ \"abc\": null, \"bcd\": {\"replace\": null} }"));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body("{\"done\": null}")
          .when()
          .patch(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("raw", "true")
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200)
          .body(jsonEquals("{ \"abc\": null, \"bcd\": {\"replace\": null}, \"done\": null }"));
    }

    @Test
    public void withProfile() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .queryParam("profile", "true")
          .body(DEFAULT_PAYLOAD)
          .when()
          .patch(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200)
          .body("profile", notNullValue());
    }

    @Test
    public void withTtlAutoNewDoc() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .queryParam("ttl-auto", "true")
          .body(DEFAULT_PAYLOAD)
          .when()
          .patch(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200);
    }

    @Test
    public void withTtlAutoExistingDoc() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .queryParam("ttl", "5")
          .body("{\"a\":\"b\"}")
          .when()
          .put(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200)
          .body("documentId", equalTo(documentId));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .queryParam("ttl-auto", "true")
          .body(DEFAULT_PAYLOAD)
          .when()
          .patch(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200);

      Awaitility.await()
          .atMost(6000, TimeUnit.MILLISECONDS)
          .untilAsserted(
              () ->
                  given()
                      .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
                      .when()
                      .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
                      .then()
                      .statusCode(404));
    }

    @Test
    public void malformedJson() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(MALFORMED_PAYLOAD)
          .when()
          .patch(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(400);
    }

    @Test
    public void emptyObject() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body("{}")
          .when()
          .patch(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body("[]")
          .when()
          .patch(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .body("code", equalTo(400))
          .body(
              "description",
              equalTo("A patch operation must be done with a JSON object, not an array."));
    }

    @Test
    public void nonEmptyArray() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body("[1,2,3]")
          .when()
          .patch(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .body("code", equalTo(400))
          .body(
              "description",
              equalTo("A patch operation must be done with a JSON object, not an array."));
    }

    @Test
    public void singlePrimitive() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body("true")
          .when()
          .patch(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .when()
          .patch(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(400)
          .body("code", equalTo(400))
          .body("description", equalTo("Request invalid: payload must not be empty."));
    }

    @Test
    public void tableNotAValidCollection() {
      String namespace = "system";
      String collection = "local";

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(DEFAULT_PAYLOAD)
          .when()
          .patch(BASE_PATH, namespace, collection, documentId)
          .then()
          .statusCode(400)
          .body("code", equalTo(400))
          .body(
              "description",
              equalTo(
                  "The database table system.local is not a Documents collection. Accessing arbitrary tables via the Documents API is not permitted."));
    }

    @Test
    public void unauthorized() {
      given()
          .contentType(ContentType.JSON)
          .body(DEFAULT_PAYLOAD)
          .when()
          .patch(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(401);
    }

    @Test
    public void keyspaceNotExists() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(DEFAULT_PAYLOAD)
          .when()
          .patch(BASE_PATH, "notakeyspace", DEFAULT_COLLECTION, documentId)
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(DEFAULT_PAYLOAD)
          .when()
          .patch(BASE_PATH + "/path", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200)
          .body("documentId", equalTo(documentId));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("raw", "true")
          .when()
          .get(BASE_PATH + "/path", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200)
          .body(jsonEquals(DEFAULT_PAYLOAD));
    }

    @Test
    public void subDocumentPatch() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body("{\"abc\": null}")
          .when()
          .put(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body("{\"bcd\": {}}}")
          .when()
          .patch(BASE_PATH + "/abc", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("raw", "true")
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200)
          .body(jsonEquals("{ \"abc\": { \"bcd\": {} }}}"));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body("3")
          .when()
          .patch(BASE_PATH + "/abc/bcd", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("raw", "true")
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200)
          .body(jsonEquals("{ \"abc\": { \"bcd\": 3 }}}"));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body("{\"bcd\": [null,2,null,4]}")
          .when()
          .patch(BASE_PATH + "/abc", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("raw", "true")
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200)
          .body(jsonEquals("{ \"abc\": {\"bcd\": [null,2,null,4]} }"));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body("{\"bcd\": [1,{\"a\": null},3,4]}")
          .when()
          .patch(BASE_PATH + "/abc", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("raw", "true")
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200)
          .body(jsonEquals("{ \"abc\": { \"bcd\": [1,{\"a\": null},3,4] }}"));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body("{\"bcd\": [null]}")
          .when()
          .patch(BASE_PATH + "/abc", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("raw", "true")
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200)
          .body(jsonEquals("{ \"abc\": { \"bcd\": [null] }}"));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body("{\"null\": null}")
          .when()
          .patch(BASE_PATH + "/abc", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("raw", "true")
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200)
          .body(jsonEquals("{ \"abc\": { \"bcd\": [null], \"null\": null }}"));
    }

    @Test
    public void testPatchWithAutoTtl() throws JsonProcessingException {
      JsonNode obj1 = objectMapper.readTree("{ \"delete this\": \"in 5 seconds\" }");
      JsonNode obj2 = objectMapper.readTree("{ \"match the parent\": \"this\", \"a\": \"b\" }");

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .queryParam("ttl", "5")
          .body(obj1)
          .when()
          .put(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .queryParam("ttl-auto", "true")
          .body(obj2)
          .when()
          .patch(BASE_PATH + "/1", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200);

      Awaitility.await()
          .atMost(6000, TimeUnit.MILLISECONDS)
          .untilAsserted(
              () -> {
                // After the TTL is up, obj1 should be gone, with no remnants
                given()
                    .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
                    .when()
                    .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(obj1)
          .when()
          .put(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .queryParam("ttl-auto", "true")
          .body(obj2)
          .when()
          .patch(BASE_PATH + "/a", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .queryParam("profile", "true")
          .body(DEFAULT_PAYLOAD)
          .when()
          .patch(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200)
          .body("profile", notNullValue());
    }

    @Test
    public void malformedJson() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(MALFORMED_PAYLOAD)
          .when()
          .patch(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(400);
    }

    @Test
    public void unauthorized() {
      given()
          .when()
          .patch(BASE_PATH + "/path", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(401);
    }

    @Test
    public void keyspaceNotExists() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(DEFAULT_PAYLOAD)
          .when()
          .patch(BASE_PATH + "/path", "notakeyspace", DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(404)
          .body("code", equalTo(404))
          .body(
              "description", equalTo("Unknown namespace notakeyspace, you must create it first."));
    }
  }
}
