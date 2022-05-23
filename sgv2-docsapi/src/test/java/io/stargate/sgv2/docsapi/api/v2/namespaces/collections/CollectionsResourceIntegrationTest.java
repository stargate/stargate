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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.stargate.sgv2.docsapi.config.constants.Constants;
import io.stargate.sgv2.docsapi.testprofiles.IntegrationTestProfile;
import io.stargate.sgv2.docsapi.testresource.StargateTestResource;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@QuarkusTestResource(StargateTestResource.class)
@TestProfile(IntegrationTestProfile.class)
public class CollectionsResourceIntegrationTest {

  // base path for the test
  public static final String BASE_PATH = "/v2/namespaces/{namespace}/collections";

  @BeforeAll
  public static void enableLog() {
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
  }

  @Nested
  class GetCollections {

    @Test
    public void happyPath() {
      // TODO once we can create keyspace
    }

    @Test
    public void happyPathRaw() {
      // TODO once we can create keyspace
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
  class CreateCollection {

    @Test
    public void happyPath() {
      // TODO once we can create keyspace
    }

    @Test
    public void tableExists() {
      // TODO once we can create keyspace
    }

    @Test
    public void tableNotExisting() {
      // TODO once we can create keyspace
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
              """
              .formatted(collection);

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
      String body =
          """
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
  class DeleteCollection {

    @Test
    public void happyPath() {
      // TODO once we can create keyspace
    }

    @Test
    public void tableNotExisting() {
      // TODO once we can create keyspace
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
