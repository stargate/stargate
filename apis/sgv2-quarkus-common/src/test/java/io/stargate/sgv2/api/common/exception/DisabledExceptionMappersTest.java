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

package io.stargate.sgv2.api.common.exception;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import java.util.Map;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(DisabledExceptionMappersTest.Profile.class)
public class DisabledExceptionMappersTest {

  public static class Profile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {

      return ImmutableMap.<String, String>builder()
          .put("stargate.exception-mappers.enabled", "false")
          .put("quarkus.http.auth.proactive", "true")
          .put("quarkus.http.auth.permission.default.paths", "/v2/*")
          .put("quarkus.http.auth.permission.default.policy", "authenticated")
          .build();
    }
  }

  @ApplicationScoped
  @Path("")
  public static class TestingResource {

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/v2/disabled-mappers")
    public String greetAuthorized() {
      throw new RuntimeException("Ignore me.");
    }
  }

  @Test
  public void noAuth() {
    // not returning ApiError, content type is not application/json
    given().when().get("/v2/disabled-mappers").then().statusCode(401).and().contentType(is(""));
  }

  @Test
  public void exception() {
    // not returning ApiError
    given()
        .headers(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "token")
        .when()
        .get("/v2/disabled-mappers")
        .then()
        .statusCode(500)
        .body("code", is(nullValue()))
        .body("description", is(nullValue()));
  }
}
