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
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import io.stargate.sgv2.docsapi.api.v2.DocsApiIntegrationTest;
import jakarta.ws.rs.core.HttpHeaders;
import java.util.Optional;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@QuarkusIntegrationTest
@QuarkusTestResource(StargateTestResource.class)
public class CollectionsResourceIntegrationTest extends DocsApiIntegrationTest {

  // base path for the test
  public static final String BASE_PATH = "/v2/namespaces/{namespace}/collections";
  public static final String DEFAULT_NAMESPACE = RandomStringUtils.randomAlphanumeric(16);
  public static final String DEFAULT_COLLECTION = RandomStringUtils.randomAlphanumeric(16);

  @Override
  public Optional<String> createNamespace() {
    return Optional.of(DEFAULT_NAMESPACE);
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .when()
          .post(BASE_PATH, namespace)
          .then()
          .statusCode(400)
          .body("code", is(400))
          .body("description", is("HTTP 400 Bad Request"));
    }

    @Test
    public void invalidPayload() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String body = "{}";

      given()
          .contentType(ContentType.JSON)
          .body(body)
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
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
          .queryParam("raw", true)
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
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
  class UpgradeCollection {

    // no way we can actually test the upgrade :(

    @Test
    public void upgradeNotPossible() {
      String json =
          """
              {
                 "upgradeType": "SAI_INDEX_UPGRADE"
              }
              """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(BASE_PATH + "/{collection}/upgrade", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(400)
          .body("code", is(400))
          .body("description", is("The collection cannot be upgraded in given manner."));
    }

    @Test
    public void upgradeNull() {
      String json = "{}";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(BASE_PATH + "/{collection}/upgrade", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(400)
          .body("code", is(400))
          .body(
              "description",
              is("Request invalid: `upgradeType` is required to upgrade a collection."));
    }

    @Test
    public void tableNotExisting() {
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String json =
          """
              {
                 "upgradeType": "SAI_INDEX_UPGRADE"
              }
              """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(BASE_PATH + "/{collection}/upgrade", DEFAULT_NAMESPACE, collection)
          .then()
          .statusCode(404)
          .body("code", is(404))
          .body("description", is("Collection '%s' not found.".formatted(collection)));
    }

    @Test
    public void keyspaceNotExisting() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String json =
          """
              {
                 "upgradeType": "SAI_INDEX_UPGRADE"
              }
              """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(BASE_PATH + "/{collection}/upgrade", namespace, collection)
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
      String json =
          """
              {
                 "upgradeType": "SAI_INDEX_UPGRADE"
              }
              """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(BASE_PATH + "/{collection}/upgrade", namespace, collection)
          .then()
          .statusCode(400)
          .body("code", is(400))
          .body(
              "description",
              is(
                  "The database table system.local is not a Documents collection. Accessing arbitrary tables via the Documents API is not permitted."));
    }
  }

  @Nested
  @Order(4)
  class DeleteCollection {

    @Test
    public void happyPath() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .when()
          .delete(BASE_PATH + "/{collection}", DEFAULT_NAMESPACE, DEFAULT_COLLECTION)
          .then()
          .statusCode(204);
    }

    @Test
    public void tableNotExisting() {
      String collection = RandomStringUtils.randomAlphanumeric(16);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
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
