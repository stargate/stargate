/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.stargate.sgv2.docsapi.api.v2.namespaces.collections.documents;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.ResourceArg;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import io.stargate.sgv2.docsapi.api.v2.DocsApiIntegrationTest;
import java.util.Optional;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@QuarkusTestResource(
    value = StargateTestResource.class,
    initArgs =
        @ResourceArg(name = StargateTestResource.Options.DISABLE_FIXED_TOKEN, value = "true"))
class DocumentDeleteResourceIntegrationTest extends DocsApiIntegrationTest {

  public static final String BASE_PATH =
      "/v2/namespaces/{namespace}/collections/{collection}/{document-id}";
  public static final String DEFAULT_NAMESPACE = RandomStringUtils.randomAlphanumeric(16);
  public static final String DEFAULT_COLLECTION = RandomStringUtils.randomAlphanumeric(16);
  public static final String DEFAULT_DOCUMENT_ID = RandomStringUtils.randomAlphanumeric(16);
  public static final String DEFAULT_PAYLOAD =
      "{\"test\": \"document\", \"this\": [\"is\", 1, true]}";

  @Override
  public Optional<String> createNamespace() {
    return Optional.of(DEFAULT_NAMESPACE);
  }

  @Override
  public Optional<String> createCollection() {
    return Optional.of(DEFAULT_COLLECTION);
  }

  public void createDocument() {
    given()
        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
        .header("Content-Type", "application/json")
        .body(DEFAULT_PAYLOAD)
        .when()
        .put(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DEFAULT_DOCUMENT_ID)
        .then()
        .statusCode(200);
  }

  @Nested
  class DeleteDocument {

    @BeforeEach
    public void setup() {
      createDocument();
    }

    @Test
    public void happyPath() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .when()
          .delete(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DEFAULT_DOCUMENT_ID)
          .then()
          .statusCode(204);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .queryParam("raw", true)
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DEFAULT_DOCUMENT_ID)
          .then()
          .statusCode(404);
    }

    @Test
    public void unauthorized() {
      given()
          .when()
          .delete(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DEFAULT_DOCUMENT_ID)
          .then()
          .statusCode(401);
    }

    @Test
    public void deleteNotFound() {
      // When a delete occurs on an unknown document, it still returns 204 No Content
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .when()
          .delete(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, "no-id")
          .then()
          .statusCode(204);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .queryParam("raw", true)
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, "no-id")
          .then()
          .statusCode(404);
    }

    @Test
    public void keyspaceNotExists() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .when()
          .delete(BASE_PATH, "notakeyspace", DEFAULT_COLLECTION, DEFAULT_DOCUMENT_ID)
          .then()
          .statusCode(404)
          .body("code", equalTo(404))
          .body(
              "description", equalTo("Unknown namespace notakeyspace, you must create it first."));
    }

    @Test
    public void tableNotExists() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .when()
          .delete(BASE_PATH, DEFAULT_NAMESPACE, "notatable", DEFAULT_DOCUMENT_ID)
          .then()
          .statusCode(404)
          .body("code", equalTo(404))
          .body("description", equalTo("Collection 'notatable' not found."));
    }
  }

  @Nested
  class DeleteDocumentPath {

    @BeforeEach
    public void setup() {
      createDocument();
    }

    @Test
    public void testDeletePath() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .when()
          .delete(BASE_PATH + "/test", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DEFAULT_DOCUMENT_ID)
          .then()
          .statusCode(204);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .queryParam("raw", true)
          .when()
          .get(BASE_PATH + "/test", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DEFAULT_DOCUMENT_ID)
          .then()
          .statusCode(404);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .queryParam("raw", true)
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DEFAULT_DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body("test", nullValue());
    }

    @Test
    public void testDeleteArrayPath() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .when()
          .delete(
              BASE_PATH + "/this/[2]", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DEFAULT_DOCUMENT_ID)
          .then()
          .statusCode(204);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .queryParam("raw", true)
          .when()
          .get(BASE_PATH + "/this", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DEFAULT_DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(jsonEquals("[\"is\",1]"));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .when()
          .delete(
              BASE_PATH + "/this/[0]", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DEFAULT_DOCUMENT_ID)
          .then()
          .statusCode(204);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .queryParam("raw", true)
          .when()
          .get(BASE_PATH + "/this", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DEFAULT_DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(jsonEquals("[null,1]"));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .queryParam("raw", true)
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DEFAULT_DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body("this", jsonEquals("[null,1]"));
    }

    @Test
    public void testDeletePathNotFound() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .when()
          .delete(BASE_PATH + "/test/a", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DEFAULT_DOCUMENT_ID)
          .then()
          .statusCode(204);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .queryParam("raw", true)
          .when()
          .get(BASE_PATH + "/test", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DEFAULT_DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(jsonEquals("\"document\""));
    }

    @Test
    public void unauthorized() {
      given()
          .when()
          .delete(BASE_PATH + "/test", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DEFAULT_DOCUMENT_ID)
          .then()
          .statusCode(401);
    }
  }
}
