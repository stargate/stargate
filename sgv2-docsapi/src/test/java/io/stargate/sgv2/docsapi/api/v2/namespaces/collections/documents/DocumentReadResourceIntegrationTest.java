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
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonPartEquals;
import static net.javacrumbs.jsonunit.core.util.ResourceUtils.resource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.CharStreams;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.stargate.sgv2.common.cql.builder.Replication;
import io.stargate.sgv2.docsapi.config.constants.Constants;
import io.stargate.sgv2.docsapi.service.schema.NamespaceManager;
import io.stargate.sgv2.docsapi.service.schema.TableManager;
import io.stargate.sgv2.docsapi.testprofiles.IntegrationTestProfile;
import java.time.Duration;
import javax.enterprise.context.control.ActivateRequestContext;
import javax.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.assertj.core.api.HamcrestCondition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@QuarkusTest
@TestProfile(IntegrationTestProfile.class)
@ActivateRequestContext
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DocumentReadResourceIntegrationTest {

  public static final String BASE_PATH = "/v2/namespaces/{namespace}/collections/{collection}";
  public static final String DEFAULT_NAMESPACE = RandomStringUtils.randomAlphanumeric(16);
  public static final String DEFAULT_COLLECTION = RandomStringUtils.randomAlphanumeric(16);

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
  class GetDocument {

    static final String DOCUMENT_ID = RandomStringUtils.randomAlphanumeric(16);

    private String exampleResource;

    @BeforeEach
    public void createDoc() throws Exception {
      exampleResource = CharStreams.toString(resource("documents/example.json"));

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(exampleResource)
          .when()
          .put(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200);
    }

    @AfterEach
    public void deleteDoc() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .delete(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(204);
    }

    // 2xx

    @Test
    public void happyPath() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(jsonPartEquals("documentId", DOCUMENT_ID))
          .body(jsonPartEquals("data", exampleResource));
    }

    @Test
    public void happyPathRaw() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(jsonEquals(exampleResource));
    }

    @Test
    public void withProfile() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("profile", true)
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body("profile", is(not(nullValue())));
    }

    @Test
    public void matchEq() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .param("where", "{\"products.electronics.Pixel_3a.price\": {\"$eq\": 600}}")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(
              jsonEquals("[{\"products\": {\"electronics\": {\"Pixel_3a\": {\"price\": 600}}}}]"));

      // not matched
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .param("where", "{\"products.electronics.Pixel_3a.price\": {\"$eq\": 700}}")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(204);
    }

    @Test
    public void matchEqWithSelection() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .param("where", "{\"products.electronics.Pixel_3a.price\": {\"$eq\": 600}}")
          .param("fields", "[\"name\", \"price\", \"model\", \"manufacturer\"]")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(
              jsonEquals(
                  "[{\"products\": {\"electronics\": {\"Pixel_3a\": {\"name\": \"Pixel\", \"manufacturer\": \"Google\", \"model\": \"3a\", \"price\": 600}}}}]"));
    }

    @Test
    public void matchLt() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .param("where", "{\"products.food.*.price\": {\"$lt\": 600}}")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(
              jsonEquals(
                  "[{\"products\": {\"food\": {\"Apple\": {\"price\": 0.99}}}}, {\"products\": {\"food\": { \"Pear\": {\"price\": 0.89}}}}]"));
    }

    @Test
    public void matchLtWithSelection() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .param("where", "{\"products.food.*.price\": {\"$lt\": 600}}")
          .param("fields", "[\"name\", \"price\", \"model\"]")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(
              jsonEquals(
                  "[{\"products\": {\"food\": {\"Apple\": {\"name\": \"apple\", \"price\": 0.99}}}}, {\"products\": {\"food\": { \"Pear\": {\"name\": \"pear\", \"price\": 0.89}}}}]"));
    }

    @Test
    public void matchLte() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .param("where", "{\"products.food.*.price\": {\"$lte\": 0.99}}")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(
              jsonEquals(
                  "[{\"products\": {\"food\": {\"Apple\": {\"price\": 0.99}}}}, {\"products\": {\"food\": { \"Pear\": {\"price\": 0.89}}}}]"));
    }

    @Test
    public void matchLteWithSelection() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .param("where", "{\"products.food.*.price\": {\"$lte\": 0.99}}")
          .param("fields", "[\"price\", \"sku\"]")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(
              jsonEquals(
                  "[{\"products\": {\"food\": {\"Apple\": {\"price\": 0.99, \"sku\": \"100100010101001\"}}}}, {\"products\": {\"food\": { \"Pear\": {\"price\": 0.89, \"sku\": null}}}}]"));
    }

    @Test
    public void matchGt() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .param("where", "{\"products.electronics.*.price\": {\"$gt\": 600}}")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(
              jsonEquals("[{\"products\": {\"electronics\": {\"iPhone_11\": {\"price\": 900}}}}]"));
    }

    @Test
    public void matchGtWithSelection() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .param("where", "{\"products.electronics.*.price\": {\"$gt\": 600}}")
          .param("fields", "[\"price\", \"throwaway\"]")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(
              jsonEquals("[{\"products\": {\"electronics\": {\"iPhone_11\": {\"price\": 900}}}}]"));
    }

    @Test
    public void matchGte() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .param("where", "{\"products.electronics.*.price\": {\"$gte\": 600}}")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(
              jsonEquals(
                  "[{\"products\": {\"electronics\": {\"Pixel_3a\": {\"price\": 600}}}}, {\"products\": {\"electronics\": { \"iPhone_11\": {\"price\": 900}}}}]"));
    }

    @Test
    public void matchGteWithSelection() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .param("where", "{\"products.electronics.*.price\": {\"$gte\": 600}}")
          .param("fields", "[\"price\"]")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(
              jsonEquals(
                  "[{\"products\": {\"electronics\": {\"Pixel_3a\": {\"price\": 600}}}}, {\"products\": {\"electronics\": { \"iPhone_11\": {\"price\": 900}}}}]"));
    }

    @Test
    public void matchExists() {
      String expected =
          """
          [
            {"products":{"electronics":{"Pixel_3a":{"price":600}}}},
            {"products":{"electronics":{"iPhone_11":{"price":900}}}},
            {"products":{"food":{"Apple":{"price":0.99}}}},
            {"products":{"food":{"Pear":{"price":0.89}}}}
          ]""";

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .param("where", "{\"products.*.*.price\": {\"$exists\": true}}")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(jsonEquals(expected));
    }

    @Test
    public void matchExistsWithSelection() {
      String expected =
          """
          [
            {"products":{"electronics":{"Pixel_3a":{"price":600, "name":"Pixel", "manufacturer":"Google", "model":"3a"}}}},
            {"products":{"electronics":{"iPhone_11":{"price":900, "name":"iPhone", "manufacturer":"Apple", "model":"11"}}}},
            {"products":{"food":{"Apple":{"name": "apple", "price":0.99, "sku": "100100010101001"}}}},
            {"products":{"food":{"Pear":{"name": "pear", "price":0.89, "sku": null}}}}
          ]""";

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .param("where", "{\"products.*.*.price\": {\"$exists\": true}}")
          .param("fields", "[\"price\", \"name\", \"manufacturer\", \"model\", \"sku\"]")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(jsonEquals(expected));
    }

    @Test
    public void matchNot() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .param("where", "{\"$not\": {\"products.electronics.Pixel_3a.price\": {\"$ne\": 600}}}")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(
              jsonEquals("[{\"products\": {\"electronics\": {\"Pixel_3a\": {\"price\": 600}}}}]"));
    }

    @Test
    public void matchNe() {
      // NE with String
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .param("where", "{\"products.electronics.*.model\": {\"$ne\": \"3a\"}}")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(
              jsonEquals(
                  "[{\"products\": {\"electronics\": { \"iPhone_11\": {\"model\": \"11\"}}}}]"));

      // NE with Boolean
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .param("where", "{\"quiz.nests.q1.options.[3].this\": {\"$ne\": false}}")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(
              jsonEquals(
                  "[{\"quiz\": {\"nests\": { \"q1\": {\"options\": {\"[3]\": {\"this\": true}}}}}}]"));

      // NE integer compared to double
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .param("where", "{\"quiz.maths.q1.answer\": {\"$ne\": 12}}")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(jsonEquals("[{\"quiz\": {\"maths\": { \"q1\": {\"answer\": 12.2}}}}]"));

      // NE with double compared to integer
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .param("where", "{\"quiz.maths.q2.answer\": {\"$ne\": 4.0}}")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(204);
    }

    @Test
    public void matchIn() {
      // IN with String
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .param("where", "{\"products.electronics.*.model\": {\"$in\": [\"11\", \"3a\"]}}")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(
              jsonEquals(
                  "[{\"products\": {\"electronics\": { \"Pixel_3a\": {\"model\": \"3a\"}}}}, {\"products\": {\"electronics\": { \"iPhone_11\": {\"model\": \"11\"}}}}]"));

      // IN with int
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .param("where", "{\"products.*.*.price\": {\"$in\": [600, 900]}}")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(
              jsonEquals(
                  "[{\"products\": {\"electronics\": { \"Pixel_3a\": {\"price\": 600}}}}, {\"products\": {\"electronics\": { \"iPhone_11\": {\"price\": 900}}}}]"));

      // IN with double
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .param("where", "{\"products.*.*.price\": {\"$in\": [0.99, 0.89]}}")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(
              jsonEquals(
                  "[{\"products\": {\"food\": { \"Apple\": {\"price\": 0.99}}}}, {\"products\": {\"food\": { \"Pear\": {\"price\": 0.89}}}}]"));

      // IN with null
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .param("where", "{\"products.*.*.sku\": {\"$in\": [null]}}")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(jsonEquals("[{\"products\": {\"food\": { \"Pear\": {\"sku\": null}}}}]"));
    }

    @Test
    public void matchNin() {
      // NIN with String
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .param("where", "{\"products.electronics.*.model\": {\"$nin\": [\"12\"]}}")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(
              jsonEquals(
                  "[{\"products\": {\"electronics\": { \"Pixel_3a\": {\"model\": \"3a\"}}}}, {\"products\": {\"electronics\": { \"iPhone_11\": {\"model\": \"11\"}}}}]"));

      // NIN with int
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .param("where", "{\"products.*.*.price\": {\"$nin\": [600, 900]}}")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(
              jsonEquals(
                  "[{\"products\": {\"food\": { \"Apple\": {\"price\": 0.99}}}}, {\"products\": {\"food\": { \"Pear\": {\"price\": 0.89}}}}]"));

      // NIN with double
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .param("where", "{\"products.*.*.price\": {\"$nin\": [0.99, 0.89]}}")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(
              jsonEquals(
                  "[{\"products\": {\"electronics\": { \"Pixel_3a\": {\"price\": 600}}}}, {\"products\": {\"electronics\": { \"iPhone_11\": {\"price\": 900}}}}]"));

      // NIN with null
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .param("where", "{\"products.*.*.sku\": {\"$nin\": [null]}}")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(
              jsonEquals(
                  "[{\"products\": {\"food\": { \"Apple\": {\"sku\": \"100100010101001\"}}}}]"));
    }

    @Test
    public void matchFiltersCombo() {
      // NIN (limited support) with GT (full support)
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .param("where", "{\"products.electronics.*.model\": {\"$nin\": [\"11\"], \"$gt\": \"\"}}")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(
              jsonEquals(
                  "[{\"products\": {\"electronics\": { \"Pixel_3a\": {\"model\": \"3a\"}}}}]"));

      // IN (limited support) with NE (limited support)
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .param("where", "{\"products.electronics.*.model\": {\"$nin\": [\"11\"], \"$gt\": \"\"}}")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(
              jsonEquals(
                  "[{\"products\": {\"electronics\": { \"Pixel_3a\": {\"model\": \"3a\"}}}}]"));
    }

    @Test
    public void matchMultipleOperators() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .param("where", "{\"products.food.Orange.info.price\": {\"$gt\": 600, \"$lt\": 600.05}}")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(
              jsonEquals(
                  "[{\"products\": {\"food\": {\"Orange\": {\"info\": {\"price\": 600.01}}}}}]"));
    }

    @Test
    public void matchArrayPaths() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .param("where", "{\"quiz.maths.q1.options.[0]\": {\"$lt\": 13.3}}")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(jsonEquals("[{\"quiz\":{\"maths\":{\"q1\":{\"options\":{\"[0]\":10.2}}}}}]"));
    }

    @Test
    public void matchMultiplePaths() {
      String expected =
          """
              [
                {"quiz":{"nests":{"q1":{"options":{"[0]":"nest"}}}}},
                {"quiz":{"nests":{"q2":{"options":{"[0]":"nest"}}}}}
              ]""";

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .param("where", "{\"quiz.nests.q1,q2.options.[0]\": {\"$eq\": \"nest\"}}")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(jsonEquals(expected));
    }

    @Test
    public void matchMultiplePathsAndGlob() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .param("where", "{\"quiz.nests.q2,q3.options.*.this.them\": {\"$eq\": false}}")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(
              jsonEquals(
                  "[{\"quiz\":{\"nests\":{\"q3\":{\"options\":{\"[2]\":{\"this\":{\"them\":false}}}}}}}]"));
    }

    @Test
    public void matchWithSelectivity() {
      String where =
          """
              {
                "products.electronics.Pixel_3a.price": {"$gte": 600},
                "products.electronics.Pixel_3a.price": {"$lte": 600, "$selectivity": 0.5}
              }""";

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .param("where", where)
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(
              jsonEquals("[{\"products\": {\"electronics\": {\"Pixel_3a\": {\"price\": 600}}}}]"));
    }

    @Test
    public void matchEscaped() {
      String id = RandomStringUtils.randomAlphanumeric(16);
      String json = "{\"a\\\\.b\":\"somedata\",\"some,data\":\"something\",\"*\":\"star\"}";

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .put(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200);

      // with escaped period
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .param("where", "{\"a\\\\.b\": {\"$eq\": \"somedata\"}}")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200)
          .body(jsonEquals("[{\"a.b\":\"somedata\"}]"));

      // with commas
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .param("where", "{\"some\\\\,data\": {\"$eq\": \"something\"}}")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200)
          .body(jsonEquals("[{\"some,data\":\"something\"}]"));

      // with asterisk
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .param("where", "{\"\\\\*\": {\"$eq\": \"star\"}}")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200)
          .body(jsonEquals("[{\"*\":\"star\"}]"));
    }

    @Test
    public void withPagination() throws Exception {
      String id = RandomStringUtils.randomAlphanumeric(16);
      exampleResource = CharStreams.toString(resource("documents/long-search.json"));

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(exampleResource)
          .when()
          .put(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200);

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("where", "{\"*.value\": {\"$gt\": 0}}")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200)
          .body("data", hasSize(100))
          .body("pageState", notNullValue());
    }

    @Test
    public void withPaginationAndGivenPageSize() throws Exception {
      String id = RandomStringUtils.randomAlphanumeric(16);
      exampleResource = CharStreams.toString(resource("documents/long-search.json"));

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(exampleResource)
          .when()
          .put(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200);

      String bodyFirstPage =
          given()
              .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
              .param("where", "{\"*.value\": {\"$gt\": 0}}")
              .param("page-size", 20)
              .when()
              .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
              .then()
              .statusCode(200)
              .body("data", hasSize(20))
              .body("pageState", notNullValue())
              .extract()
              .body()
              .asString();

      // paging only second with state
      String pageState = objectMapper.readTree(bodyFirstPage).requiredAt("/pageState").textValue();
      String bodySecondPage =
          given()
              .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
              .param("where", "{\"*.value\": {\"$gt\": 0}}")
              .param("page-size", 20)
              .param("page-state", pageState)
              .when()
              .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
              .then()
              .statusCode(200)
              .body("data", hasSize(20))
              .extract()
              .body()
              .asString();

      // second page should have no data matching first page
      JsonNode data1 = objectMapper.readTree(bodyFirstPage).requiredAt("/data");
      JsonNode data2 = objectMapper.readTree(bodySecondPage).requiredAt("/data");
      assertThat(data1).doesNotContainAnyElementsOf(data2);
    }

    // 4xx

    @Test
    public void matchInvalid() {
      // not JSON
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("where", "hello")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(400);

      // array
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("where", "[\"a\"]")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(400)
          .body("description", is("Search was expecting a JSON object as input."));

      // no-op
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("where", "{\"a\": true}")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(400)
          .body("description", is("A filter operation and value resolved as invalid."));

      // op not found
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("where", "{\"a\": {\"exists\": true}}}")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(400)
          .body("description", is("Operation 'exists' is not supported."));

      // op value not valid
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("where", "{\"a\": {\"$eq\": null}}}")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(400)
          .body("description", is("Operation '$eq' does not support the provided value null."));

      // op value empty
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("where", "{\"a\": {\"$eq\": {}}}}")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(400)
          .body("description", is("Operation '$eq' does not support the provided value { }."));

      // op value array
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("where", "{\"a\": {\"$eq\": []}}}")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(400)
          .body("description", is("Operation '$eq' does not support the provided value [ ]."));

      // in not array
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("where", "{\"a\": {\"$in\": 2}}}")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(400)
          .body("description", is("Operation '$in' does not support the provided value 2."));

      // multiple field conditions
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("where", "{\"a\": {\"$eq\": 300}, \"b\": {\"$lt\": 500}}")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(400)
          .body(
              "description",
              is("Conditions across multiple fields are not yet supported. Found: 2."));
    }

    @Test
    public void matchWhereAndFieldsDifferentTarget() {
      // not JSON
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("where", "{\"a\": {\"$in\": [1]}}")
          .param("fields", "[\"b\"]")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(400)
          .body(
              "description",
              is(
                  "When selecting `fields`, the field referenced by `where` must be in the selection."));
    }

    @Test
    public void matchPageSizeNotPositive() {
      // not JSON
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("page-size", 0)
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(400)
          .body(
              "description",
              is("Request invalid: the minimum number of results to return is one."));
    }

    @Test
    public void whereMalformed() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("where", "{\"a\":}")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(400)
          .body("code", is(400))
          .body(
              "description",
              is(
                  "The `where` parameter expects a valid JSON object representing search criteria."));
    }

    @Test
    public void fieldsMalformed() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("fields", "[\"a\"")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .body("code", is(400))
          .body(
              "description",
              is("The `fields` parameter expects a valid JSON array containing field names."));
    }

    @Test
    public void keyspaceNotExisting() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH + "/{document-id}", namespace, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(404)
          .body("code", is(404))
          .body(
              "description",
              is("Unknown namespace %s, you must create it first.".formatted(namespace)));
    }

    @Test
    public void tableNotExisting() {
      String collection = RandomStringUtils.randomAlphanumeric(16);

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, collection, DOCUMENT_ID)
          .then()
          .statusCode(404)
          .body("code", is(404))
          .body("description", is("Collection '%s' not found.".formatted(collection)));
    }

    @Test
    public void documentNotExisting() {
      String id = RandomStringUtils.randomAlphanumeric(16);

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(404)
          .body("code", is(404))
          .body("description", is("A document with the id %s does not exist.".formatted(id)));
    }

    @Test
    public void tableNotAValidCollection() {
      String namespace = "system";
      String collection = "local";

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .delete(BASE_PATH + "/{document-id}", namespace, collection, DOCUMENT_ID)
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
      given()
          .when()
          .get(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(401);
    }
  }

  @Nested
  class GetDocumentPath {

    static final String DOCUMENT_ID = RandomStringUtils.randomAlphanumeric(16);

    private String exampleResource;

    @BeforeEach
    public void createDoc() throws Exception {
      exampleResource = CharStreams.toString(resource("documents/example.json"));

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(exampleResource)
          .when()
          .put(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(200);
    }

    @AfterEach
    public void deleteDoc() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .delete(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, DOCUMENT_ID)
          .then()
          .statusCode(204);
    }

    // 2xx

    @Test
    public void happyPath() throws Exception {
      Object dataMap =
          given()
              .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
              .when()
              .get(
                  BASE_PATH + "/{document-id}/quiz/maths",
                  DEFAULT_NAMESPACE,
                  DEFAULT_COLLECTION,
                  DOCUMENT_ID)
              .then()
              .statusCode(200)
              .body(jsonPartEquals("documentId", DOCUMENT_ID))
              .extract()
              .path("data");

      String data = objectMapper.writeValueAsString(dataMap);
      HamcrestCondition<String> condition =
          new HamcrestCondition<>(jsonPartEquals("quiz.maths", data));
      assertThat(exampleResource).is(condition);
    }

    @Test
    public void happyPathRaw() {
      String body =
          given()
              .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
              .param("raw", true)
              .when()
              .get(
                  BASE_PATH + "/{document-id}/quiz/maths",
                  DEFAULT_NAMESPACE,
                  DEFAULT_COLLECTION,
                  DOCUMENT_ID)
              .then()
              .statusCode(200)
              .extract()
              .body()
              .asString();

      HamcrestCondition<String> condition =
          new HamcrestCondition<>(jsonPartEquals("quiz.maths", body));
      assertThat(exampleResource).is(condition);
    }

    @Test
    public void array() {
      String first =
          given()
              .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
              .param("raw", true)
              .when()
              .get(
                  BASE_PATH + "/{document-id}/quiz/maths/q1/options/[0]",
                  DEFAULT_NAMESPACE,
                  DEFAULT_COLLECTION,
                  DOCUMENT_ID)
              .then()
              .statusCode(200)
              .extract()
              .body()
              .asString();

      HamcrestCondition<String> condition =
          new HamcrestCondition<>(jsonPartEquals("quiz.maths.q1.options[0]", first));
      assertThat(exampleResource).is(condition);

      String second =
          given()
              .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
              .param("raw", true)
              .when()
              .get(
                  BASE_PATH + "/{document-id}/quiz/nests/q1/options/[3]/this",
                  DEFAULT_NAMESPACE,
                  DEFAULT_COLLECTION,
                  DOCUMENT_ID)
              .then()
              .statusCode(200)
              .extract()
              .body()
              .asString();

      HamcrestCondition<String> condition2 =
          new HamcrestCondition<>(jsonPartEquals("quiz.nests.q1.options[3].this", second));
      assertThat(exampleResource).is(condition2);
    }

    @Test
    public void escapedChar() {
      String id = RandomStringUtils.randomAlphanumeric(16);
      String json = "{\"a\\\\.b\":\"somedata\",\"some,data\":\"something\",\"*\":\"star\"}";

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .put(BASE_PATH + "/{document-id}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200);

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .when()
          .get(BASE_PATH + "/{document-id}/a%5C.b", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200)
          .body(jsonEquals("somedata"));

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .when()
          .get(BASE_PATH + "/{document-id}/some%5C,data", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200)
          .body(jsonEquals("something"));

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .when()
          .get(BASE_PATH + "/{document-id}/%5C*", DEFAULT_NAMESPACE, DEFAULT_COLLECTION, id)
          .then()
          .statusCode(200)
          .body(jsonEquals("star"));
    }

    @Test
    public void matchOrWithSelection() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .param(
              "where",
              "{\"$or\":[{\"*.name\":{\"$eq\":\"pear\"}},{\"*.name\":{\"$eq\":\"orange\"}}]}")
          .param("fields", "[\"name\"]")
          .when()
          .get(
              BASE_PATH + "/{document-id}/products/food",
              DEFAULT_NAMESPACE,
              DEFAULT_COLLECTION,
              DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(
              jsonEquals(
                  "[{\"products\": {\"food\": {\"Orange\": {\"name\": \"orange\"}}}}, {\"products\": {\"food\": {\"Pear\": {\"name\": \"pear\"}}}}]"));
    }

    @Test
    public void matchOrWithPaging() throws Exception {
      String body =
          given()
              .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
              .param(
                  "where",
                  "{\"$or\":[{\"*.name\":{\"$eq\":\"pear\"}},{\"*.name\":{\"$eq\":\"orange\"}}]}")
              .param("fields", "[\"name\"]")
              .param("page-size", 1)
              .when()
              .get(
                  BASE_PATH + "/{document-id}/products/food",
                  DEFAULT_NAMESPACE,
                  DEFAULT_COLLECTION,
                  DOCUMENT_ID)
              .then()
              .statusCode(200)
              .body("pageState", notNullValue())
              .body(
                  jsonPartEquals(
                      "data", "[{\"products\": {\"food\": {\"Orange\": {\"name\": \"orange\"}}}}]"))
              .extract()
              .body()
              .asString();

      // paging only second with state
      String pageState = objectMapper.readTree(body).requiredAt("/pageState").textValue();
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param(
              "where",
              "{\"$or\":[{\"*.name\":{\"$eq\":\"pear\"}},{\"*.name\":{\"$eq\":\"orange\"}}]}")
          .param("fields", "[\"name\"]")
          .param("page-size", 1)
          .param("page-state", pageState)
          .when()
          .get(
              BASE_PATH + "/{document-id}/products/food",
              DEFAULT_NAMESPACE,
              DEFAULT_COLLECTION,
              DOCUMENT_ID)
          .then()
          .statusCode(200)
          .body(
              jsonPartEquals(
                  "data", "[{\"products\": {\"food\": {\"Pear\": {\"name\": \"pear\"}}}}]"));
    }

    // 4xx

    @Test
    public void invalidPath() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .when()
          .get(
              BASE_PATH + "/{document-id}/nonexistent/path",
              DEFAULT_NAMESPACE,
              DEFAULT_COLLECTION,
              DOCUMENT_ID)
          .then()
          .statusCode(404);

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .when()
          .get(
              BASE_PATH + "/{document-id}/nonexistent/path/[1]",
              DEFAULT_NAMESPACE,
              DEFAULT_COLLECTION,
              DOCUMENT_ID)
          .then()
          .statusCode(404);

      // out of bounds
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .param("raw", true)
          .when()
          .get(
              BASE_PATH + "/{document-id}/quiz/maths/q1/options/[9999]",
              DEFAULT_NAMESPACE,
              DEFAULT_COLLECTION,
              DOCUMENT_ID)
          .then()
          .statusCode(404);
    }
  }
}
