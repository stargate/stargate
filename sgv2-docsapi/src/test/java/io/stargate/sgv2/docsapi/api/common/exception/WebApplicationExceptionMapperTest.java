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

package io.stargate.sgv2.docsapi.api.common.exception;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

import io.quarkus.test.junit.QuarkusTest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class WebApplicationExceptionMapperTest {

  @Path("web-application-exception-mapper-test")
  public static class TestingResource {

    @GET
    @Produces("text/plain")
    public String greet() {
      return "hello";
    }
  }

  @Test
  public void happyPath() {
    given()
        .when()
        .post("/web-application-exception-mapper-test")
        .then()
        .statusCode(405)
        .body("code", is(405))
        .body("description", is("HTTP 405 Method Not Allowed"));
  }
}
