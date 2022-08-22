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
import static net.javacrumbs.jsonunit.JsonMatchers.jsonPartEquals;
import static org.hamcrest.Matchers.equalTo;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.ResourceArg;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import io.stargate.sgv2.docsapi.api.v2.DocsApiIntegrationTest;
import java.util.Optional;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@QuarkusTestResource(
    value = StargateTestResource.class,
    initArgs =
        @ResourceArg(name = StargateTestResource.Options.DISABLE_FIXED_TOKEN, value = "true"))
class BuiltInFunctionResourceIntegrationTest extends DocsApiIntegrationTest {

  public static final String BASE_PATH =
      "/v2/namespaces/{namespace}/collections/{collection}/{document-id}";
  public static final String DEFAULT_NAMESPACE = RandomStringUtils.randomAlphanumeric(16);
  public static final String DEFAULT_COLLECTION = RandomStringUtils.randomAlphanumeric(16);
  public static final String DOCUMENT_ID = RandomStringUtils.randomAlphanumeric(16);

  @Override
  public Optional<String> createNamespace() {
    return Optional.of(DEFAULT_NAMESPACE);
  }

  @Override
  public Optional<String> createCollection() {
    return Optional.of(DEFAULT_COLLECTION);
  }

  @Nested
  class ExecuteBuiltInFunction {

    public static final String PUSH_PAYLOAD = "{\"operation\": \"$push\", \"value\": true}";

    public static final String POP_PAYLOAD = "{\"operation\": \"$pop\"}";

    @BeforeEach
    public void setup() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body("{\"array\": [1, 2, 3], \"object\": {}}")
          .when()
          .put(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200);
    }

    @AfterEach
    public void cleanUp() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .when()
          .delete(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(204);
    }

    @Test
    public void pop() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(POP_PAYLOAD)
          .when()
          .post(BASE_PATH + "/array/function", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body("documentId", equalTo(DOCUMENT_ID))
          .body("data", jsonEquals(3));

      // assert whole document
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body("documentId", equalTo(DOCUMENT_ID))
          .body("data", jsonPartEquals("array", "[1, 2]"));
    }

    @Test
    public void popRaw() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .queryParam("raw", true)
          .contentType(ContentType.JSON)
          .body(POP_PAYLOAD)
          .when()
          .post(BASE_PATH + "/array/function", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(jsonEquals(3));
    }

    @Test
    public void popEmpty() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(POP_PAYLOAD)
          .when()
          .post(BASE_PATH + "/array/function", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body("documentId", equalTo(DOCUMENT_ID))
          .body("data", jsonEquals(3));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(POP_PAYLOAD)
          .when()
          .post(BASE_PATH + "/array/function", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body("documentId", equalTo(DOCUMENT_ID))
          .body("data", jsonEquals(2));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(POP_PAYLOAD)
          .when()
          .post(BASE_PATH + "/array/function", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body("documentId", equalTo(DOCUMENT_ID))
          .body("data", jsonEquals(1));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(POP_PAYLOAD)
          .when()
          .post(BASE_PATH + "/array/function", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(400)
          .body("code", equalTo(400))
          .body("description", equalTo("No data available to pop."));
    }

    @Test
    public void popNoArray() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(POP_PAYLOAD)
          .when()
          .post(BASE_PATH + "/object/function", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(400)
          .body("code", equalTo(400))
          .body("description", equalTo("The path provided to pop from has no array, found {}."));
    }

    @Test
    public void push() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(PUSH_PAYLOAD)
          .when()
          .post(BASE_PATH + "/array/function", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body("documentId", equalTo(DOCUMENT_ID))
          .body("data", jsonEquals("[1, 2, 3, true]"));

      // assert whole document
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body("documentId", equalTo(DOCUMENT_ID))
          .body("data", jsonPartEquals("array", "[1, 2, 3, true]"));
    }

    @Test
    public void pushObject() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body("{\"operation\": \"$push\", \"value\": { \"p\": true}}")
          .when()
          .post(BASE_PATH + "/array/function", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body("documentId", equalTo(DOCUMENT_ID))
          .body("data", jsonEquals("[1, 2, 3, { \"p\": true}]"));

      // assert whole document
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body("documentId", equalTo(DOCUMENT_ID))
          .body("data", jsonPartEquals("array", "[1, 2, 3, { \"p\": true}]"));
    }

    @Test
    public void pushArray() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body("{\"operation\": \"$push\", \"value\": [4, 5, 6]}")
          .when()
          .post(BASE_PATH + "/array/function", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body("documentId", equalTo(DOCUMENT_ID))
          .body("data", jsonEquals("[1, 2, 3, [4, 5, 6]]"));

      // assert whole document
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body("documentId", equalTo(DOCUMENT_ID))
          .body("data", jsonPartEquals("array", "[1, 2, 3, [4, 5, 6]]"));
    }

    @Test
    public void pushRaw() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .queryParam("raw", true)
          .contentType(ContentType.JSON)
          .body(PUSH_PAYLOAD)
          .when()
          .post(BASE_PATH + "/array/function", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(jsonEquals("[1, 2, 3, true]"));
    }

    @Test
    public void pushNull() {
      String payload = "{\"operation\": \"$push\", \"value\": null}";

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(payload)
          .when()
          .post(BASE_PATH + "/array/function", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body("documentId", equalTo(DOCUMENT_ID))
          .body("data", jsonEquals("[1, 2, 3, null]"));
    }

    @Test
    public void pushNoArray() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(PUSH_PAYLOAD)
          .when()
          .post(BASE_PATH + "/object/function", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(400)
          .body("code", equalTo(400))
          .body("description", equalTo("The path provided to push to has no array, found {}."));
    }

    @Test
    public void invalidOperation() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body("{\"operation\": \"$dollar\"}")
          .when()
          .post(BASE_PATH + "/array/function", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(400)
          .body("code", equalTo(400))
          .body(
              "description",
              equalTo("Request invalid: available built-in functions are $pop and $push."));
    }

    @Test
    public void notExistingDocument() {
      String id = RandomStringUtils.randomAlphanumeric(16);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(PUSH_PAYLOAD)
          .when()
          .post(BASE_PATH + "/array/function", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(404)
          .body("code", equalTo(404))
          .body(
              "description",
              equalTo(
                  "A path [array] in a document with the id %s, or the document itself, does not exist."
                      .formatted(id)));
    }

    @Test
    public void invalidCollection() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(PUSH_PAYLOAD)
          .when()
          .post(BASE_PATH + "/array/function", DEFAULT_NAMESPACE, "missingcollection", DOCUMENT_ID)
          .then()
          .statusCode(404)
          .body("code", equalTo(404))
          .body("description", equalTo("Collection 'missingcollection' not found."));
    }

    @Test
    public void invalidNamespace() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(PUSH_PAYLOAD)
          .when()
          .post(BASE_PATH + "/array/function", "missingnamespace", DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(404)
          .body("code", equalTo(404))
          .body(
              "description",
              equalTo("Unknown namespace missingnamespace, you must create it first."));
    }

    @Test
    public void tableNotAValidCollection() {
      String namespace = "system";
      String collection = "local";

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(PUSH_PAYLOAD)
          .when()
          .post(BASE_PATH + "/array/function", namespace, collection, DOCUMENT_ID)
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
          .body(PUSH_PAYLOAD)
          .when()
          .post(BASE_PATH + "/array/function", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(401);
    }
  }
}
