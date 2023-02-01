package io.stargate.sgv2.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Integration tests that verify it is possible to disable CQL endpoint (at {@code /v2/cql}). */
@QuarkusIntegrationTest
@QuarkusTestResource(StargateTestResource.class)
@TestProfile(RestApiV2QCqlDisabledIT.Profile.class)
public class RestApiV2QCqlDisabledIT extends RestApiV2QIntegrationTestBase {
  // Although /cql endpoint may be disabled by default let's make sure
  public static class Profile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of("stargate.rest.cql.disabled", "true");
    }
  }

  public RestApiV2QCqlDisabledIT() {
    super("cqld_ks_", "cqld_t_", KeyspaceCreation.NONE);
  }

  @Test
  public void testCqlQueryWhenEndpointDisabled() {
        givenWithAuth()
            .contentType(ContentType.TEXT)
            .body("SELECT key FROM system.local")
            .when()
            .post(endpointPathForCQL())
            .then()
            .statusCode(404);
  }
}
