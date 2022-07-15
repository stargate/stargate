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
import static net.javacrumbs.jsonunit.JsonMatchers.jsonPartEquals;
import static net.javacrumbs.jsonunit.core.util.ResourceUtils.resource;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import com.google.common.io.CharStreams;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.api.common.cql.builder.Replication;
import io.stargate.sgv2.docsapi.service.schema.CollectionManager;
import io.stargate.sgv2.docsapi.service.schema.NamespaceManager;
import io.stargate.sgv2.docsapi.testprofiles.IntegrationTestProfile;
import java.time.Duration;
import javax.enterprise.context.control.ActivateRequestContext;
import javax.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@QuarkusTest
@TestProfile(IntegrationTestProfile.class)
@ActivateRequestContext
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DocumentUpdateResourceIntegrationTest {

  public static final String BASE_PATH =
      "/v2/namespaces/{namespace}/collections/{collection}/{document-id}";
  public static final String DEFAULT_NAMESPACE = RandomStringUtils.randomAlphanumeric(16);
  public static final String DEFAULT_COLLECTION = RandomStringUtils.randomAlphanumeric(16);

  @Inject NamespaceManager namespaceManager;

  @Inject CollectionManager collectionManager;

  @BeforeAll
  public void init() {
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();

    namespaceManager
        .createNamespace(DEFAULT_NAMESPACE, Replication.simpleStrategy(1))
        .await()
        .atMost(Duration.ofSeconds(10));

    collectionManager
        .createCollectionTable(DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
        .await()
        .atMost(Duration.ofSeconds(10));
  }

  @Nested
  class UpdateDocument {

    String exampleResource;

    @BeforeEach
    public void loadJson() throws Exception {
      exampleResource = CharStreams.toString(resource("documents/example.json"));
    }

    @Test
    public void happyPath() {
      String id = RandomStringUtils.randomAlphanumeric(16);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(exampleResource)
          .when()
          .put(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .body(jsonPartEquals("documentId", id))
          .body(jsonPartEquals("data", exampleResource));
    }

    @Test
    public void withProfile() {
      String id = RandomStringUtils.randomAlphanumeric(16);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("profile", true)
          .contentType(ContentType.JSON)
          .body(exampleResource)
          .when()
          .put(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200)
          .body("profile", is(notNullValue()));
    }

    @Test
    public void withTtl() {
      String id = RandomStringUtils.randomAlphanumeric(16);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("ttl", 3)
          .contentType(ContentType.JSON)
          .body(exampleResource)
          .when()
          .put(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200);

      await()
          .atLeast(Duration.ofSeconds(1))
          .atMost(Duration.ofSeconds(5))
          .untilAsserted(
              () ->
                  given()
                      .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
                      .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
                      .then()
                      .statusCode(404));
    }

    @Test
    public void withEscapedChars() {
      String id = RandomStringUtils.randomAlphanumeric(16);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body("{ \"periods\\\\.\": \"are allowed if escaped\" }")
          .when()
          .put(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .body(jsonPartEquals("documentId", id))
          .body(jsonPartEquals("data", "{\"periods.\": \"are allowed if escaped\" }"));

      // overwrite with asterisks
      String asterisksJson = "{ \"*aste*risks*\": \"are allowed\" }";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(asterisksJson)
          .when()
          .put(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .body(jsonPartEquals("documentId", id))
          .body(jsonPartEquals("data", asterisksJson));
    }

    @ParameterizedTest
    @CsvSource({"{\"abc\": null}", "{\"abc\": {}}", "{\"abc\": []}"})
    public void withNullsAndEmpties(String json) {
      String id = RandomStringUtils.randomAlphanumeric(16);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .put(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .body(jsonPartEquals("documentId", id))
          .body(jsonPartEquals("data", json));
    }

    @Test
    public void withNullsAndEmptiesMixed() {
      String id = RandomStringUtils.randomAlphanumeric(16);
      String json =
          "{\"abc\": [], \"bcd\": {}, \"cde\": null, \"abcd\": { \"nest1\": [], \"nest2\": {}}}";

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .put(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .body(jsonPartEquals("documentId", id))
          .body(jsonPartEquals("data", json));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .get(BASE_PATH + "/abc", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .body(jsonPartEquals("documentId", id))
          .body(jsonPartEquals("data", "[]"));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .get(BASE_PATH + "/abcd/nest1", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .body(jsonPartEquals("documentId", id))
          .body(jsonPartEquals("data", "[]"));
    }

    @Test
    public void withArray() {
      String id = RandomStringUtils.randomAlphanumeric(16);
      String json = "[1, 2, 3, 4, 5]";

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .put(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .body(jsonPartEquals("documentId", id))
          .body(jsonPartEquals("data", json));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .get(BASE_PATH + "/[000002]", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .body(jsonPartEquals("documentId", id))
          .body(jsonPartEquals("data", "3"));
    }

    // 4xx

    @Test
    public void invalidTtl() {
      String id = RandomStringUtils.randomAlphanumeric(16);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("ttl", -1)
          .contentType(ContentType.JSON)
          .body(exampleResource)
          .when()
          .put(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(400)
          .body("code", is(400))
          .body("description", is("Request invalid: TTL value must be a positive integer."));
    }

    @Test
    public void invalidDepth() throws Exception {
      String id = RandomStringUtils.randomAlphanumeric(16);
      String json = CharStreams.toString(resource("documents/too-deep.json"));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .put(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(400)
          .body("code", is(400))
          .body("description", is("Max depth of 64 exceeded."));
    }

    @Test
    public void invalidKey() {
      String id = RandomStringUtils.randomAlphanumeric(16);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body("{ \"bracketedarraypaths[100]\": \"are not allowed\" }")
          .when()
          .put(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(400)
          .body("code", is(400))
          .body(
              "description",
              is(
                  "Array paths contained in square brackets, periods, single quotes, and backslash are not allowed in field names, invalid field bracketedarraypaths[100]"));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body("{ \"periods.something\": \"are not allowed\" }")
          .when()
          .put(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(400)
          .body("code", is(400))
          .body(
              "description",
              is(
                  "Array paths contained in square brackets, periods, single quotes, and backslash are not allowed in field names, invalid field periods.something"));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body("{ \"single'quotes\": \"are not allowed\" }")
          .when()
          .put(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(400)
          .body("code", is(400))
          .body(
              "description",
              is(
                  "Array paths contained in square brackets, periods, single quotes, and backslash are not allowed in field names, invalid field single'quotes"));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body("{ \"back\\\\\\\\slashes\": \"are not allowed\" }")
          .when()
          .put(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(400)
          .body("code", is(400))
          .body(
              "description",
              is(
                  "Array paths contained in square brackets, periods, single quotes, and backslash are not allowed in field names, invalid field back\\\\slashes"));
    }

    @ParameterizedTest
    @CsvSource({"true", "2", "4.4", "null", "\"value\""})
    public void bodyPrimitive(String primitiveJson) {
      String id = RandomStringUtils.randomAlphanumeric(16);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(primitiveJson)
          .when()
          .put(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(400)
          .body("code", is(400))
          .body(
              "description",
              is(
                  "Updating a key with just a JSON primitive is not allowed. Hint: update the parent path with a defined object instead."));
    }

    @Test
    public void bodyMalformed() {
      String id = RandomStringUtils.randomAlphanumeric(16);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body("{\"some\":")
          .when()
          .put(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(400)
          .body("code", is(400))
          .body("description", startsWith("Unable to process JSON"));
    }

    @Test
    public void bodyNotExisting() {
      String id = RandomStringUtils.randomAlphanumeric(16);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .when()
          .put(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(400)
          .body("code", is(400))
          .body("description", is("Request invalid: payload must not be empty."));
    }

    @Test
    public void keyspaceNotExisting() {
      String id = RandomStringUtils.randomAlphanumeric(16);
      String namespace = RandomStringUtils.randomAlphanumeric(16);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(exampleResource)
          .when()
          .put(BASE_PATH, namespace, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(404)
          .body("code", is(404))
          .body(
              "description",
              is("Unknown namespace %s, you must create it first.".formatted(namespace)));
    }

    @Test
    public void tableNotAValidCollection() {
      String id = RandomStringUtils.randomAlphanumeric(16);
      String namespace = "system";
      String collection = "local";

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(exampleResource)
          .when()
          .put(BASE_PATH, namespace, collection, id)
          .then()
          .statusCode(400)
          .body("code", is(400))
          .body(
              "description",
              is(
                  "The database table system.local is not a Documents collection. Accessing arbitrary tables via the Documents API is not permitted."));
    }

    @Test
    public void unauthorized() {
      String id = RandomStringUtils.randomAlphanumeric(16);

      given()
          .contentType(ContentType.JSON)
          .body(exampleResource)
          .when()
          .put(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(401);
    }
  }

  @Nested
  class UpdateSubDocument {

    String exampleResource;

    @BeforeEach
    public void loadJson() throws Exception {
      exampleResource = CharStreams.toString(resource("documents/example.json"));
    }

    @Test
    public void withProfile() {
      String id = RandomStringUtils.randomAlphanumeric(16);

      String updateJson = "{\"q5000\": \"hello?\"}";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("profile", true)
          .contentType(ContentType.JSON)
          .body(updateJson)
          .when()
          .put(BASE_PATH + "/quiz/sport", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200)
          .body("profile", is(notNullValue()));
    }

    @Test
    public void replaceObject() {
      String id = RandomStringUtils.randomAlphanumeric(16);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(exampleResource)
          .when()
          .put(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200);

      String updateJson = "{\"q5000\": \"hello?\"}";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(updateJson)
          .when()
          .put(BASE_PATH + "/quiz/sport", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200);

      // assert at full doc
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .body(jsonPartEquals("data.quiz.sport", updateJson));

      // assert at path
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH + "/quiz/sport", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .body(jsonPartEquals("data", updateJson));
    }

    @Test
    public void replaceArrayElement() {
      String id = RandomStringUtils.randomAlphanumeric(16);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(exampleResource)
          .when()
          .put(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200);

      String updateJson = "{\"q5000\": \"hello?\"}";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(updateJson)
          .when()
          .put(BASE_PATH + "/quiz/nests/q1/options/[0]", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200);

      // assert at full doc
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .body(jsonPartEquals("data.quiz.nests.q1.options[0]", updateJson));

      // assert at path
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH + "/quiz/nests/q1/options/[0]", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .body(jsonPartEquals("data", updateJson));
    }

    @Test
    public void replaceWithArrayBackAndForth() {
      String id = RandomStringUtils.randomAlphanumeric(16);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(exampleResource)
          .when()
          .put(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200);

      // replace object with array
      String updateJson = "[{\"array\": \"at\"}, \"sub\", \"doc\"]";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(updateJson)
          .when()
          .put(BASE_PATH + "/quiz", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200);

      // assert at full doc
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .body(jsonPartEquals("data.quiz", updateJson));

      // assert at path
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH + "/quiz", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .body(jsonPartEquals("data", updateJson));

      // then replace array with a pre path
      updateJson = "[0, \"a\", \"2\", true]";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(updateJson)
          .when()
          .put(BASE_PATH + "/quiz/nests/q1", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200);

      // assert at full doc
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .body(jsonPartEquals("data.quiz.nests.q1", updateJson));

      // assert at path
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH + "/quiz/nests/q1", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .body(jsonPartEquals("data", updateJson));

      // then again with the array
      updateJson = "[{\"array\": \"at\"}, \"\", \"doc\"]";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(updateJson)
          .when()
          .put(BASE_PATH + "/quiz", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200);

      // assert at full doc
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .body(jsonPartEquals("data.quiz", updateJson));

      // assert at path
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH + "/quiz", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .body(jsonPartEquals("data", updateJson));

      // and then with final object
      updateJson = "{\"we\": {\"are\": \"done\"}}";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(updateJson)
          .when()
          .put(BASE_PATH + "/quiz", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200);

      // assert at full doc
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .body(jsonPartEquals("data.quiz", updateJson));

      // assert at path
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH + "/quiz", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .body(jsonPartEquals("data", updateJson));
    }

    @ParameterizedTest
    @CsvSource({"true", "2", "4.4", "null", "\"value\""})
    public void replaceWithPrimitive(String updateJson) {
      String id = RandomStringUtils.randomAlphanumeric(16);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(exampleResource)
          .when()
          .put(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(updateJson)
          .when()
          .put(BASE_PATH + "/quiz", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200);

      // assert at full doc
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .body(jsonPartEquals("data.quiz", updateJson));

      // assert at path
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH + "/quiz", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .body(jsonPartEquals("data", updateJson));
    }

    @ParameterizedTest
    @CsvSource({"true", "2", "4.4", "null", "\"value\""})
    public void replaceWithPrimitiveBackAndForth(String primitiveJson) {
      String id = RandomStringUtils.randomAlphanumeric(16);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(exampleResource)
          .when()
          .put(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(primitiveJson)
          .when()
          .put(BASE_PATH + "/quiz", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200);

      String objectJson = "{\"some\": \"value\"}";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(objectJson)
          .when()
          .put(BASE_PATH + "/quiz", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200);

      // assert at full doc
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .body(jsonPartEquals("data.quiz", objectJson));

      // assert at path
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH + "/quiz", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .body(jsonPartEquals("data", objectJson));
    }

    @Test
    public void withTtlAuto() {
      String id = RandomStringUtils.randomAlphanumeric(16);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("ttl", 2)
          .contentType(ContentType.JSON)
          .body("{ \"delete this\": \"in 2 seconds\" }")
          .when()
          .put(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("ttl-auto", true)
          .contentType(ContentType.JSON)
          .body("{ \"match the parent\": \"this\", \"a\": \"b\" }")
          .when()
          .put(BASE_PATH + "/b", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200);

      await()
          .atMost(Duration.ofSeconds(3))
          .untilAsserted(
              () ->
                  given()
                      .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
                      .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
                      .then()
                      .statusCode(404));
    }

    @Test
    public void withoutTtlAuto() {
      String id = RandomStringUtils.randomAlphanumeric(16);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("ttl", 2)
          .contentType(ContentType.JSON)
          .body("{ \"delete this\": \"in 2 seconds\" }")
          .when()
          .put(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200);

      String addedJson = "{ \"match the parent\": \"this\", \"a\": \"b\" }";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(addedJson)
          .when()
          .put(BASE_PATH + "/b", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200);

      await()
          .atMost(Duration.ofSeconds(3))
          .untilAsserted(
              () ->
                  given()
                      .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
                      .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
                      .then()
                      .statusCode(200)
                      .body(jsonPartEquals("documentId", id))
                      .body(jsonPartEquals("data.b", addedJson)));
    }

    @Test
    public void weirdButAllowedKeys() {
      String id = RandomStringUtils.randomAlphanumeric(16);

      // $
      String json = "{ \"$\": \"weird but allowed\" }";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .put(BASE_PATH + "/path", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200);
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH + "/path", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .body(jsonPartEquals("data", json));

      json = "{ \"$30\": \"not as weird\" }";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .put(BASE_PATH + "/path", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200);
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH + "/path", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .body(jsonPartEquals("data", json));

      // @
      json = "{ \"@\": \"weird but allowed\" }";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .put(BASE_PATH + "/path", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200);
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH + "/path", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .body(jsonPartEquals("data", json));

      json = "{ \"meet me @ the place\": \"not as weird\" }";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .put(BASE_PATH + "/path", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200);
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH + "/path", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .body(jsonPartEquals("data", json));

      // ?
      json = "{ \"?\": \"weird but allowed\" }";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .put(BASE_PATH + "/path", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200);
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH + "/path", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .body(jsonPartEquals("data", json));

      // spaces
      json = "{ \"spac es\": \"weird but allowed\" }";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .put(BASE_PATH + "/path", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200);
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH + "/path", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .body(jsonPartEquals("data", json));

      // number
      json = "{ \"3\": [\"totally allowed\"] }";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .put(BASE_PATH + "/path", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200);
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH + "/path", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .body(jsonPartEquals("data", json));
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH + "/path/3/[0]", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .body(jsonPartEquals("data", "\"totally allowed\""));

      json = "{ \"-1\": \"totally allowed\" }";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .put(BASE_PATH + "/path", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200);
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH + "/path", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .body(jsonPartEquals("data", json));
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH + "/path/-1", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .body(jsonPartEquals("data", "\"totally allowed\""));

      // esc
      json = "{ \"Eric says \\\"hello\\\"\": \"totally allowed\" }";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .put(BASE_PATH + "/path", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200);
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH + "/path", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .body(jsonPartEquals("data", json));
    }

    // 4xx

    @Test
    public void invalidArrayLength() {
      String id = RandomStringUtils.randomAlphanumeric(16);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body("{ \"some\": \"json\" }")
          .when()
          .put(BASE_PATH + "/1/[1000000]", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(400)
          .body("code", is(400))
          .body("description", is("Max array length of 1000000 exceeded."));
    }
  }
}
