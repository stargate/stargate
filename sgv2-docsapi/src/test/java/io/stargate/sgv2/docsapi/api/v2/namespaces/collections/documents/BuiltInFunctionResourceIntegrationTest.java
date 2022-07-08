package io.stargate.sgv2.docsapi.api.v2.namespaces.collections.documents;

import static io.restassured.RestAssured.given;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.equalTo;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.RestAssured;
import io.stargate.sgv2.common.cql.builder.Replication;
import io.stargate.sgv2.docsapi.config.constants.Constants;
import io.stargate.sgv2.docsapi.service.schema.NamespaceManager;
import io.stargate.sgv2.docsapi.service.schema.TableManager;
import io.stargate.sgv2.docsapi.testprofiles.IntegrationTestProfile;
import java.time.Duration;
import javax.enterprise.context.control.ActivateRequestContext;
import javax.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@QuarkusTest
@TestProfile(IntegrationTestProfile.class)
@ActivateRequestContext
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BuiltInFunctionResourceIntegrationTest {

  public static final String BASE_PATH =
      "/v2/namespaces/{namespace}/collections/{collection}/{document-id}";
  public static final String DEFAULT_NAMESPACE = RandomStringUtils.randomAlphanumeric(16);
  public static final String DEFAULT_COLLECTION = RandomStringUtils.randomAlphanumeric(16);
  public String documentId;
  public static final String POP_PAYLOAD = "{\"operation\": \"$pop\"}";
  public static final String PUSH_PAYLOAD = "{\"operation\": \"$push\", \"value\": true}";
  public static final String PUSH_PAYLOAD_NULL = "{\"operation\": \"$push\", \"value\": null}";

  @Inject NamespaceManager namespaceManager;

  @Inject TableManager tableManager;

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
    given()
        .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
        .header("Content-Type", "application/json")
        .body("{\"array\": [1, 2, 3], \"object\": {}}")
        .when()
        .put(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
        .then()
        .statusCode(200)
        .body("documentId", equalTo(documentId));

    given()
        .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
        .when()
        .get(BASE_PATH + "/array", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
        .then()
        .statusCode(200)
        .body("data", jsonEquals("[1, 2, 3]"));
  }

  @Nested
  class PushToDocument {
    @Test
    public void happyPath() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .body(PUSH_PAYLOAD)
          .when()
          .post(BASE_PATH + "/array/function", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200)
          .body("documentId", equalTo(documentId))
          .body("data", jsonEquals("[1, 2, 3, true]"));
    }

    @Test
    public void happyPathWriteNull() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .body(PUSH_PAYLOAD_NULL)
          .when()
          .post(BASE_PATH + "/array/function", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200)
          .body("documentId", equalTo(documentId))
          .body("data", jsonEquals("[1, 2, 3, null]"));
    }

    @Test
    public void pushNoArray() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .body(PUSH_PAYLOAD)
          .when()
          .post(BASE_PATH + "/object/function", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(400)
          .body("code", equalTo(400))
          .body("description", equalTo("The path provided to push to has no array, found {}"));
    }

    @Test
    public void invalidOperation() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .body("{\"operation\": \"$dollar\"}")
          .when()
          .post(BASE_PATH + "/array/function", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(400)
          .body("code", equalTo(400))
          .body("description", equalTo("Invalid Built-In function name."));
    }

    @Test
    public void invalidCollection() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .body(PUSH_PAYLOAD)
          .when()
          .post(BASE_PATH + "/array/function", DEFAULT_NAMESPACE, "missingcollection", documentId)
          .then()
          .statusCode(404)
          .body("code", equalTo(404))
          .body("description", equalTo("Collection 'missingcollection' not found."));
    }

    @Test
    public void invalidNamespace() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .body(PUSH_PAYLOAD)
          .when()
          .post(BASE_PATH + "/array/function", "missingnamespace", DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(404)
          .body("code", equalTo(404))
          .body(
              "description",
              equalTo("Unknown namespace missingnamespace, you must create it first."));
    }
  }

  @Nested
  class PopFromDocument {
    @Test
    public void happyPath() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .body(POP_PAYLOAD)
          .when()
          .post(BASE_PATH + "/array/function", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200)
          .body("documentId", equalTo(documentId))
          .body("data", jsonEquals(3));
    }

    @Test
    public void popEmpty() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .body(POP_PAYLOAD)
          .when()
          .post(BASE_PATH + "/array/function", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200)
          .body("documentId", equalTo(documentId))
          .body("data", jsonEquals(3));

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .body(POP_PAYLOAD)
          .when()
          .post(BASE_PATH + "/array/function", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200)
          .body("documentId", equalTo(documentId))
          .body("data", jsonEquals(2));

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .body(POP_PAYLOAD)
          .when()
          .post(BASE_PATH + "/array/function", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(200)
          .body("documentId", equalTo(documentId))
          .body("data", jsonEquals(1));

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .body(POP_PAYLOAD)
          .when()
          .post(BASE_PATH + "/array/function", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(400)
          .body("code", equalTo(400))
          .body("description", equalTo("No data available to pop."));
    }

    @Test
    public void popNoArray() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .header("Content-Type", "application/json")
          .body(POP_PAYLOAD)
          .when()
          .post(BASE_PATH + "/object/function", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, documentId)
          .then()
          .statusCode(400)
          .body("code", equalTo(400))
          .body("description", equalTo("The path provided to pop from has no array, found {}"));
    }
  }
}
