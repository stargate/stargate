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

package io.stargate.sgv2.docsapi.api.v2.namespaces.collections.jsonschema;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.common.IntegrationTestUtils;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import io.stargate.sgv2.docsapi.api.v2.DocsApiIntegrationTest;
import io.stargate.sgv2.docsapi.api.v2.namespaces.collections.CollectionsResource;
import java.util.Optional;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@QuarkusIntegrationTest
@QuarkusTestResource(StargateTestResource.class)
public class JsonSchemaResourceIntegrationTest extends DocsApiIntegrationTest {

  // base path for the test
  public static final String BASE_PATH =
      "/v2/namespaces/{namespace}/collections/{collection}/json-schema";
  public static final String DEFAULT_NAMESPACE = RandomStringUtils.randomAlphanumeric(16);
  public static final String DEFAULT_COLLECTION = RandomStringUtils.randomAlphanumeric(16);

  @Override
  public Optional<String> createNamespace() {
    return Optional.of(DEFAULT_NAMESPACE);
  }

  @Override
  public Optional<String> createCollection() {
    return Optional.of(DEFAULT_COLLECTION);
  }

  @Nested
  @Order(1)
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class PutJsonSchema {

    @Test
    @Order(1)
    public void happyPath() {
      String body =
          """
            {"$schema": "https://json-schema.org/draft/2019-09/schema"}
          """;

      given()
          .contentType(ContentType.JSON)
          .header(
              HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, IntegrationTestUtils.getAuthToken())
          .body(body)
          .put(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(200)
          .body("schema.$schema", is("https://json-schema.org/draft/2019-09/schema"));
    }

    @Test
    @Order(2)
    public void updateExisting() {
      String body =
          """
            {"$schema": "https://json-schema.org/draft/2020-09/schema"}
          """;

      given()
          .contentType(ContentType.JSON)
          .header(
              HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, IntegrationTestUtils.getAuthToken())
          .body(body)
          .put(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(200)
          .body("schema.$schema", is("https://json-schema.org/draft/2020-09/schema"));
    }

    @Test
    @Order(3)
    public void tableDoesNotExist() {
      String body =
          """
            {"$schema": "https://json-schema.org/draft/2019-09/schema"}
          """;

      given()
          .contentType(ContentType.JSON)
          .header(
              HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, IntegrationTestUtils.getAuthToken())
          .body(body)
          .put(BASE_PATH, DEFAULT_NAMESPACE, "notatable")
          .then()
          .statusCode(404)
          .body("code", is(404))
          .body("description", is("Collection 'notatable' not found."));
    }

    @Test
    @Order(4)
    public void keyspaceDoesNotExist() {
      String body =
          """
            {"$schema": "https://json-schema.org/draft/2019-09/schema"}
          """;

      given()
          .contentType(ContentType.JSON)
          .header(
              HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, IntegrationTestUtils.getAuthToken())
          .body(body)
          .put(BASE_PATH, "notexist", DEFAULT_COLLECTION)
          .then()
          .statusCode(404)
          .body("code", is(404))
          .body("description", is("Unknown namespace notexist, you must create it first."));
    }

    @Test
    @Order(5)
    public void invalidJsonSchema() {
      String body =
          """
              {
                "$schema": "https://json-schema.org/draft/2019-09/schema",
                "type": "object",
                "properties": {
                  "something": { "type": "strin" }
                }
              }""";

      given()
          .contentType(ContentType.JSON)
          .body(body)
          .header(
              HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, IntegrationTestUtils.getAuthToken())
          .when()
          .put(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(400)
          .body("code", is(400))
          .body(
              "description",
              is(
                  "[error]: \"strin\" is not a valid primitive type (valid values are: [array, boolean, integer, null, number, object, string]); "));
    }

    @Test
    @Order(6)
    public void noPayload() {
      given()
          .contentType(ContentType.JSON)
          .header(
              HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, IntegrationTestUtils.getAuthToken())
          .when()
          .put(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(400)
          .body("code", is(400))
          .body("description", is("Request invalid: json schema not provided."));
    }

    @Test
    @Order(7)
    public void notDocumentTable() {
      given()
          .contentType(ContentType.JSON)
          .body("{}")
          .header(
              HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, IntegrationTestUtils.getAuthToken())
          .when()
          .put(BASE_PATH, "system", "peers")
          .then()
          .statusCode(400)
          .body("code", is(400))
          .body(
              "description",
              is(
                  "The database table system.peers is not a Documents collection. Accessing arbitrary tables via the Documents API is not permitted."));
    }
  }

  @Nested
  @Order(2)
  class GetJsonSchema {

    @Test
    @Order(1)
    public void happyPath() {
      given()
          .header(
              HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, IntegrationTestUtils.getAuthToken())
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(200)
          .body("schema.$schema", is("https://json-schema.org/draft/2020-09/schema"));
    }

    @Test
    @Order(2)
    public void noSchemaAvailable() {
      // Create a fresh table for this test, to have no schema
      String json =
          """
              {
                  "name": "freshtable"
              }
              """;
      given()
          .contentType(ContentType.JSON)
          .header(
              HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, IntegrationTestUtils.getAuthToken())
          .body(json)
          .post(CollectionsResource.BASE_PATH, DEFAULT_NAMESPACE);

      given()
          .header(
              HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, IntegrationTestUtils.getAuthToken())
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, "freshtable")
          .then()
          .statusCode(404)
          .body("code", is(404))
          .body("description", is("The JSON schema is not set for the collection."));
    }

    @Test
    @Order(3)
    public void tableNotExisting() {

      given()
          .header(
              HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, IntegrationTestUtils.getAuthToken())
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, "notatable")
          .then()
          .statusCode(404)
          .body("code", is(404))
          .body("description", is("Collection 'notatable' not found."));
    }

    @Test
    @Order(4)
    public void keyspaceNotExisting() {

      given()
          .header(
              HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, IntegrationTestUtils.getAuthToken())
          .when()
          .get(BASE_PATH, "notexist", DEFAULT_COLLECTION)
          .then()
          .statusCode(404)
          .body("code", is(404))
          .body("description", is("Unknown namespace notexist, you must create it first."));
    }

    @Test
    @Order(5)
    public void notDocumentTable() {
      given()
          .header(
              HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, IntegrationTestUtils.getAuthToken())
          .when()
          .get(BASE_PATH, "system", "peers")
          .then()
          .statusCode(400)
          .body("code", is(400))
          .body(
              "description",
              is(
                  "The database table system.peers is not a Documents collection. Accessing arbitrary tables via the Documents API is not permitted."));
    }
  }
}
