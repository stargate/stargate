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

package io.stargate.sgv2.docsapi.api.v2.namespaces.collections;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.stargate.sgv2.common.cql.builder.Replication;
import io.stargate.sgv2.docsapi.config.constants.Constants;
import io.stargate.sgv2.docsapi.service.schema.NamespaceManager;
import io.stargate.sgv2.docsapi.testprofiles.IntegrationTestProfile;
import java.time.Duration;
import javax.enterprise.context.control.ActivateRequestContext;
import javax.inject.Inject;
import javax.ws.rs.core.HttpHeaders;
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
public class CollectionsResourceIntegrationTest {

  // TODO replace with HTTP call and remove @Inject to support end-to-end
  //  remove @ActivateRequestContext
  //  remove @TestInstance(TestInstance.Lifecycle.PER_CLASS)
  //  drop keyspace properly, see https://github.com/quarkusio/quarkus/issues/25812
  //  could be as the junit extension that runs before all tests

  // base path for the test
  public static final String BASE_PATH = "/v2/namespaces/{namespace}/collections";
  public static final String DEFAULT_NAMESPACE = RandomStringUtils.randomAlphanumeric(16);
  public static final String DEFAULT_COLLECTION = RandomStringUtils.randomAlphanumeric(16);

  @Inject NamespaceManager namespaceManager;

  @BeforeAll
  public void init() {
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();

    namespaceManager
        .createNamespace(DEFAULT_NAMESPACE, Replication.simpleStrategy(1))
        .await()
        .atMost(Duration.ofSeconds(10));
  }

  @Nested
  @Order(1)
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class CreateCollection {

    @Test
    @Order(1)
    public void happyPath() {
      String body =
          """
          {
              "name": "%s"
          }
          """
              .formatted(DEFAULT_COLLECTION);

      given()
          .contentType(ContentType.JSON)
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .body(body)
          .post(BASE_PATH, DEFAULT_NAMESPACE)
          .then()
          .header(
              HttpHeaders.LOCATION,
              endsWith(
                  "/v2/namespaces/%s/collections/%s"
                      .formatted(DEFAULT_NAMESPACE, DEFAULT_COLLECTION)))
          .statusCode(201);
    }

    @Test
    @Order(2)
    public void tableExists() {
      String body =
          """
          {
              "name": "%s"
          }
          """
              .formatted(DEFAULT_COLLECTION);

      given()
          .contentType(ContentType.JSON)
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .body(body)
          .post(BASE_PATH, DEFAULT_NAMESPACE)
          .then()
          .statusCode(409)
          .body("code", is(409))
          .body(
              "description",
              is("Create failed: collection %s already exists.".formatted(DEFAULT_COLLECTION)));
    }

    @Test
    public void keyspaceNotExisting() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String body =
          """
          {
            "name": "%s"
          }
          """.formatted(collection);

      given()
          .contentType(ContentType.JSON)
          .body(body)
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .post(BASE_PATH, namespace)
          .then()
          .statusCode(404)
          .body("code", is(404))
          .body(
              "description",
              is("Unknown namespace %s, you must create it first.".formatted(namespace)));
    }

    @Test
    public void noPayload() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);

      given()
          .contentType(ContentType.JSON)
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .post(BASE_PATH, namespace)
          .then()
          .statusCode(400)
          .body("code", is(400))
          .body("description", is("Request invalid: payload not provided."));
    }

    @Test
    public void malformedPayload() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String body = """
          {
            "malformed":
          }
          """;

      given()
          .contentType(ContentType.JSON)
          .body(body)
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .post(BASE_PATH, namespace)
          .then()
          .statusCode(400)
          .body("code", is(400))
          .body("description", startsWith("Unable to process JSON"));
    }

    @Test
    public void invalidPayload() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String body = "{}";

      given()
          .contentType(ContentType.JSON)
          .body(body)
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .post(BASE_PATH, namespace)
          .then()
          .statusCode(400)
          .body("code", is(400))
          .body("description", is("Request invalid: `name` is required to create a collection."));
    }
  }

  @Nested
  @Order(2)
  class GetCollections {

    @Test
    public void happyPath() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE)
          .then()
          .statusCode(200)
          .body("data[0].name", is(DEFAULT_COLLECTION))
          .body("data[0].upgradeAvailable", is(false))
          .body("data[0].upgradeType", is(nullValue()));
    }

    @Test
    public void happyPathRaw() {
      given()
          .param("raw", true)
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH, DEFAULT_NAMESPACE)
          .then()
          .statusCode(200)
          .body("[0].name", is(DEFAULT_COLLECTION))
          .body("[0].upgradeAvailable", is(false))
          .body("[0].upgradeType", is(nullValue()));
    }

    @Test
    public void notCollectionTables() {
      String namespace = "system";

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH, namespace)
          .then()
          .statusCode(200)
          .body("data", is(empty()));
    }

    @Test
    public void keyspaceNotExisting() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH, namespace)
          .then()
          .statusCode(404)
          .body("code", is(404))
          .body(
              "description",
              is("Unknown namespace %s, you must create it first.".formatted(namespace)));
    }
  }

  @Nested
  @Order(3)
  class DeleteCollection {

    @Test
    public void happyPath() {
      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .delete(BASE_PATH + "/{collection}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(204);
    }

    @Test
    public void tableNotExisting() {
      String collection = RandomStringUtils.randomAlphanumeric(16);

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .delete(BASE_PATH + "/{collection}", DEFAULT_NAMESPACE, collection)
          .then()
          .statusCode(404)
          .body("code", is(404))
          .body("description", is("Collection '%s' not found.".formatted(collection)));
    }

    @Test
    public void keyspaceNotExisting() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .delete(BASE_PATH + "/{collection}", namespace, collection)
          .then()
          .statusCode(404)
          .body("code", is(404))
          .body(
              "description",
              is("Unknown namespace %s, you must create it first.".formatted(namespace)));
    }

    @Test
    public void tableNotAValidCollection() {
      String namespace = "system";
      String collection = "local";

      given()
          .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .delete(BASE_PATH + "/{collection}", namespace, collection)
          .then()
          .statusCode(400)
          .body("code", is(400))
          .body(
              "description",
              is(
                  "The database table system.local is not a Documents collection. Accessing arbitrary tables via the Documents API is not permitted."));
    }
  }
}
