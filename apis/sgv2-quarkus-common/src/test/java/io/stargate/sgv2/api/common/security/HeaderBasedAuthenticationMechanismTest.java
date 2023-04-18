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

package io.stargate.sgv2.api.common.security;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import java.util.Map;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(HeaderBasedAuthenticationMechanismTest.Profile.class)
public class HeaderBasedAuthenticationMechanismTest {

  public static final class Profile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
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
    @Path("/v2/testing")
    public String greetAuthorized() {
      return "hello-authorized";
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/testing")
    public String greet() {
      return "hello";
    }
  }

  @Test
  public void missingToken() {
    given()
        .when()
        .get("/v2/testing")
        .then()
        .statusCode(401)
        .body("code", is(401))
        .body(
            "description",
            is(
                "Role unauthorized for operation: Missing token, expecting one in the X-Cassandra-Token header."));
  }

  @Test
  public void notProtectedPaths() {
    given().when().get("/testing").then().statusCode(200);
  }
}
