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

package io.stargate.sgv2.docsapi.api.v2.schemas.namespaces;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.docsapi.testprofiles.IntegrationTestProfile;
import javax.ws.rs.core.HttpHeaders;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(IntegrationTestProfile.class)
class NamespacesResourceIntegrationTest {

  public static final String BASE_PATH = "/v2/schemas/namespaces";

  @BeforeAll
  public static void enableLog() {
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
  }

  @Nested
  class GetAllNamespaces {

    @Test
    public void happyPath() {
      // expect at least one keyspace, like system
      // rest assured does not support wildcards, so validate only first
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH)
          .then()
          .statusCode(200)
          .body("data", is(not(empty())))
          .body("data[0].name", is(not(empty())));
    }

    @Test
    public void happyPathRaw() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("raw", true)
          .when()
          .get(BASE_PATH)
          .then()
          .statusCode(200)
          .body("[0].name", is(not(empty())));
    }
  }

  @Nested
  class GetNamespace {

    @Test
    public void happyPathRaw() {
      String namespace = "system";

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .queryParam("raw", true)
          .when()
          .get(BASE_PATH + "/{namespace}", namespace)
          .then()
          .statusCode(200)
          .body("name", is(namespace));
    }

    @Test
    public void notExisting() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH + "/{namespace}", namespace)
          .then()
          .statusCode(404)
          .body("code", is(404))
          .body("description", is("Namespace %s does not exist.".formatted(namespace)));
    }
  }

  @Nested
  class CreateNamespace {

    // note that test with datacenters can not be done due to the
    // https://github.com/apache/cassandra/blob/cassandra-4.0.0/NEWS.txt#L190-L195

    @Test
    public void happyPath() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String json =
          """
          {
              "name": "%s"
          }
          """.formatted(namespace);

      // create
      given()
          .contentType(ContentType.JSON)
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .body(json)
          .post(BASE_PATH)
          .then()
          .header(HttpHeaders.LOCATION, endsWith("/v2/schemas/namespaces/%s".formatted(namespace)))
          .statusCode(201);

      // assert
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH + "/{namespace}", namespace)
          .then()
          .statusCode(200)
          .body("data.name", is(namespace))
          .body("data.replicas", is(1))
          .body("data.datacenters", is(nullValue()));
    }

    @Test
    public void happyPathWithReplicas() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String json =
          """
          {
              "name": "%s",
              "replicas": 3
          }
          """
              .formatted(namespace);

      // create
      given()
          .contentType(ContentType.JSON)
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .body(json)
          .post(BASE_PATH)
          .then()
          .statusCode(201);

      // assert
      // note that in the tests, we only have a single node,
      // so we can not expect 3 replicas
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH + "/{namespace}", namespace)
          .then()
          .statusCode(200)
          .body("data.name", is(namespace))
          .body("data.replicas", allOf(greaterThan(0), lessThanOrEqualTo(3)))
          .body("data.datacenters", is(nullValue()));
    }
  }

  @Nested
  class DeleteNamespace {

    @Test
    public void happyPath() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String json =
          """
          {
              "name": "%s"
          }
          """.formatted(namespace);

      // create
      given()
          .contentType(ContentType.JSON)
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .body(json)
          .post(BASE_PATH)
          .then()
          .statusCode(201);

      // delete
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .delete(BASE_PATH + "/{namespace}", namespace)
          .then()
          .statusCode(204);

      // assert deleted
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .get(BASE_PATH + "/{namespace}", namespace)
          .then()
          .statusCode(404);
    }

    @Test
    public void notExisting() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
          .when()
          .delete(BASE_PATH + "/{namespace}", namespace)
          .then()
          .statusCode(404)
          .body("code", is(404))
          .body("description", is("Namespace %s does not exist.".formatted(namespace)));
    }
  }
}
