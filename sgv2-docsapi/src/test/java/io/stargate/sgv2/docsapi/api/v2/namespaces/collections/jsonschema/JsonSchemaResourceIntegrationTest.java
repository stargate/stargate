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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

@QuarkusTest
@TestProfile(IntegrationTestProfile.class)
@ActivateRequestContext
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class JsonSchemaResourceIntegrationTest {

  // TODO replace with HTTP call and remove @Inject to support end-to-end
  //  remove @ActivateRequestContext
  //  remove @TestInstance(TestInstance.Lifecycle.PER_CLASS)
  //  drop keyspace properly, see https://github.com/quarkusio/quarkus/issues/25812
  //  could be as the junit extension that runs before all tests

  // base path for the test
  public static final String BASE_PATH =
      "/v2/namespaces/{namespace}/collections/{collection}/json-schema";
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
  @Order(1)
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class PutJsonSchema {

    @Test
    @Order(1)
    public void happyPath() throws JsonProcessingException {
      String body =
          """
                        {\"$schema\": \"https://json-schema.org/draft/2019-09/schema\"}
                    """;

      given()
          .contentType(ContentType.JSON)
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .body(body)
          .put(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(200)
          .body("schema.$schema", is("https://json-schema.org/draft/2019-09/schema"));
    }

    @Test
    @Order(2)
    public void updateExisting() throws JsonProcessingException {
      String body =
          """
                            {\"$schema\": \"https://json-schema.org/draft/2020-09/schema\"}
                        """;

      given()
          .contentType(ContentType.JSON)
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
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
            {\"$schema\": \"https://json-schema.org/draft/2019-09/schema\"}
          """;

      given()
          .contentType(ContentType.JSON)
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
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
                {\"$schema\": \"https://json-schema.org/draft/2019-09/schema\"}
              """;

      given()
          .contentType(ContentType.JSON)
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
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
          "{\n"
              + "  \"$schema\": \"https://json-schema.org/draft/2019-09/schema\",\n"
              + "  \"type\": \"object\",\n"
              + "  \"properties\": {\n"
              + "    \"something\": { \"type\": \"strin\" }\n"
              + "  }\n"
              + "}";

      given()
          .contentType(ContentType.JSON)
          .body(body)
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
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
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
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
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
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
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(200)
          .body("schema.$schema", is("https://json-schema.org/draft/2020-09/schema"));
    }

    @Test
    @Order(2)
    public void tableNotExisting() {

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE, "notatable")
          .then()
          .statusCode(404)
          .body("code", is(404))
          .body("description", is("Collection 'notatable' not found."));
    }

    @Test
    @Order(3)
    public void keyspaceNotExisting() {

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH, "notexist", DEFAULT_COLLECTION)
          .then()
          .statusCode(404)
          .body("code", is(404))
          .body("description", is("Unknown namespace notexist, you must create it first."));
    }

    @Test
    @Order(4)
    public void notDocumentTable() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
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
