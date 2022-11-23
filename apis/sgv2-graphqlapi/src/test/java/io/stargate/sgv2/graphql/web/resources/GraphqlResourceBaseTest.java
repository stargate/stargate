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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.sgv2.graphql.web.resources;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.is;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.RestAssured;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import java.io.File;
import java.nio.charset.StandardCharsets;
import javax.ws.rs.core.MediaType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class GraphqlResourceBaseTest {

  @BeforeAll
  public static void initRestAssured() {
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
  }

  @Test
  public void shouldGet() {
    given()
        .queryParam("query", "{ greeting(name: \"world\") }")
        .when()
        .get("/test/graphql")
        .then()
        .statusCode(200)
        .body(is("{\"data\":{\"greeting\":\"hello, world\"}}"));
  }

  @Test
  public void shouldGetWithVariables() {
    given()
        .queryParam("query", "query ParameterizedGreeting($name: String) { greeting(name: $name) }")
        .queryParam("variables", "{ \"name\": \"world\" }")
        // Not strictly needed but since it can be passed as well:
        .queryParam("operationName", "ParameterizedGreeting")
        .when()
        .get("/test/graphql")
        .then()
        .statusCode(200)
        .body(is("{\"data\":{\"greeting\":\"hello, world\"}}"));
  }

  @Test
  public void shouldPostJson() {
    given()
        .body(
            "{ "
                + "\"query\": \"query ParameterizedGreeting($name: String) { greeting(name: $name) }\", "
                + "\"variables\": { \"name\": \"world\" } "
                + "}")
        .contentType(MediaType.APPLICATION_JSON)
        .when()
        .post("/test/graphql")
        .then()
        .statusCode(200)
        .body(is("{\"data\":{\"greeting\":\"hello, world\"}}"));
  }

  @Test
  public void shouldPostJsonWithOperationAsQueryParam() {
    given()
        .queryParam("query", "query ParameterizedGreeting($name: String) { greeting(name: $name) }")
        .body("{ \"variables\": { \"name\": \"world\" } }")
        .contentType(MediaType.APPLICATION_JSON)
        .when()
        .post("/test/graphql")
        .then()
        .statusCode(200)
        .body(is("{\"data\":{\"greeting\":\"hello, world\"}}"));
  }

  @Test
  public void shouldPostGraphql() {
    given()
        .body("{ greeting(name: \"world\") }".getBytes(StandardCharsets.UTF_8))
        .contentType(GraphqlResourceBase.APPLICATION_GRAPHQL)
        .when()
        .post("/test/graphql")
        .then()
        .statusCode(200)
        .body(is("{\"data\":{\"greeting\":\"hello, world\"}}"));
  }

  @Test
  public void shouldPostMultipartJson() {
    given()
        .multiPart(
            "operations",
            "{ "
                + "\"query\": \"query ParameterizedGreeting($file1: Upload) { greetingFromFile(file: $file1) }\", "
                + "\"variables\": { \"file1\": \"this_will_be_replaced_by_the_file_s_contents\" } "
                + "}",
            MediaType.APPLICATION_JSON)
        .multiPart("map", "{ \"part1\": [ \"variables.file1\" ] }", MediaType.APPLICATION_JSON)
        .multiPart("part1", new File("src/test/resources/file_for_multipart_test.txt"))
        .contentType(MediaType.MULTIPART_FORM_DATA)
        .when()
        .post("/test/graphql")
        .then()
        .statusCode(200)
        .body(is("{\"data\":{\"greetingFromFile\":\"hello, world\"}}"));
  }
}
