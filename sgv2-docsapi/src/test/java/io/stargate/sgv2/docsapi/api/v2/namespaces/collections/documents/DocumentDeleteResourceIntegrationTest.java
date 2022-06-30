package io.stargate.sgv2.docsapi.api.v2.namespaces.collections.documents;

import static io.restassured.RestAssured.given;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.RestAssured;
import io.stargate.sgv2.common.cql.builder.Replication;
import io.stargate.sgv2.docsapi.config.constants.Constants;
import io.stargate.sgv2.docsapi.service.ExecutionContext;
import io.stargate.sgv2.docsapi.service.schema.NamespaceManager;
import io.stargate.sgv2.docsapi.service.schema.TableManager;
import io.stargate.sgv2.docsapi.service.write.WriteDocumentsService;
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
class DocumentDeleteResourceIntegrationTest {

  public static final String BASE_PATH = "/v2/namespaces/{namespace}/collections/{collection}";
  public static final String DEFAULT_NAMESPACE = RandomStringUtils.randomAlphanumeric(16);
  public static final String DEFAULT_COLLECTION = RandomStringUtils.randomAlphanumeric(16);
  public static final String DEFAULT_DOCUMENT_ID = RandomStringUtils.randomAlphanumeric(16);
  public static final String DEFAULT_PAYLOAD =
      "{\"test\": \"document\", \"this\": [\"is\", 1, true]}";

  @Inject NamespaceManager namespaceManager;

  @Inject TableManager tableManager;

  @Inject WriteDocumentsService writeDocumentsService;

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
  public void setup() throws JsonProcessingException {
    writeDocumentsService
        .updateDocument(
            tableManager.getValidCollectionTable(DEFAULT_NAMESPACE, DEFAULT_COLLECTION),
            DEFAULT_NAMESPACE,
            DEFAULT_COLLECTION,
            DEFAULT_DOCUMENT_ID,
            objectMapper.readTree(DEFAULT_PAYLOAD),
            null,
            ExecutionContext.NOOP_CONTEXT)
        .await()
        .atMost(Duration.ofSeconds(10));
  }

  @Nested
  class DeleteDocument {
    @Test
    public void unauthorized() {
      given()
          .when()
          .delete(
              BASE_PATH + "/{document-id}",
              DEFAULT_NAMESPACE,
              DEFAULT_COLLECTION,
              DEFAULT_DOCUMENT_ID)
          .then()
          .statusCode(401);
    }

    @Test
    public void testDelete() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .delete(
              BASE_PATH + "/{document-id}",
              DEFAULT_NAMESPACE,
              DEFAULT_COLLECTION,
              DEFAULT_DOCUMENT_ID)
          .then()
          .statusCode(204);

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .when()
          .get(
              BASE_PATH + "/{document-id}",
              DEFAULT_NAMESPACE,
              DEFAULT_COLLECTION,
              DEFAULT_DOCUMENT_ID)
          .then()
          .statusCode(404);
    }

    @Test
    public void testDeleteNotFound() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .when()
          .get(BASE_PATH + "/no-id", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(404);

      // When a delete occurs on an unknown document, it still returns 204 No Content
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .delete(BASE_PATH + "/no-id", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(204);

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .when()
          .get(BASE_PATH + "/no-id", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(404);
    }
  }

  @Nested
  class DeleteDocumentPath {

    @Test
    public void unauthorized() {
      given()
          .when()
          .delete(
              BASE_PATH + "/{document-id}/test",
              DEFAULT_NAMESPACE,
              DEFAULT_COLLECTION,
              DEFAULT_DOCUMENT_ID)
          .then()
          .statusCode(401);
    }

    @Test
    public void testDeletePath() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .when()
          .get(
              BASE_PATH + "/{document-id}/test",
              DEFAULT_NAMESPACE,
              DEFAULT_COLLECTION,
              DEFAULT_DOCUMENT_ID)
          .then()
          .statusCode(200);

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .delete(
              BASE_PATH + "/{document-id}/test",
              DEFAULT_NAMESPACE,
              DEFAULT_COLLECTION,
              DEFAULT_DOCUMENT_ID)
          .then()
          .statusCode(204);

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .when()
          .get(
              BASE_PATH + "/{document-id}/test",
              DEFAULT_NAMESPACE,
              DEFAULT_COLLECTION,
              DEFAULT_DOCUMENT_ID)
          .then()
          .statusCode(404);
    }

    @Test
    public void testDeleteArrayPath() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .when()
          .get(
              BASE_PATH + "/{document-id}/this",
              DEFAULT_NAMESPACE,
              DEFAULT_COLLECTION,
              DEFAULT_DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(jsonEquals("[\"is\",1,true]"));

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .delete(
              BASE_PATH + "/{document-id}/this/[2]",
              DEFAULT_NAMESPACE,
              DEFAULT_COLLECTION,
              DEFAULT_DOCUMENT_ID)
          .then()
          .statusCode(204);

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .when()
          .get(
              BASE_PATH + "/{document-id}/this",
              DEFAULT_NAMESPACE,
              DEFAULT_COLLECTION,
              DEFAULT_DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(jsonEquals("[\"is\",1]"));

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .delete(
              BASE_PATH + "/{document-id}/this/[0]",
              DEFAULT_NAMESPACE,
              DEFAULT_COLLECTION,
              DEFAULT_DOCUMENT_ID)
          .then()
          .statusCode(204);

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .when()
          .get(
              BASE_PATH + "/{document-id}/this",
              DEFAULT_NAMESPACE,
              DEFAULT_COLLECTION,
              DEFAULT_DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(jsonEquals("[null,1]"));
    }

    @Test
    public void testDeletePathNotFound() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .when()
          .get(
              BASE_PATH + "/{document-id}/test",
              DEFAULT_NAMESPACE,
              DEFAULT_COLLECTION,
              DEFAULT_DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(jsonEquals("\"document\""));

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .delete(
              BASE_PATH + "/{document-id}/test/a",
              DEFAULT_NAMESPACE,
              DEFAULT_COLLECTION,
              DEFAULT_DOCUMENT_ID)
          .then()
          .statusCode(204);

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .when()
          .get(
              BASE_PATH + "/{document-id}/test",
              DEFAULT_NAMESPACE,
              DEFAULT_COLLECTION,
              DEFAULT_DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(jsonEquals("\"document\""));
    }
  }
}
