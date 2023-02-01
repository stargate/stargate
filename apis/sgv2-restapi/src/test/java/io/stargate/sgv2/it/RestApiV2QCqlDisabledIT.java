package io.stargate.sgv2.it;


import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import org.junit.jupiter.api.Test;

/** Integration tests that verify that CQL endpoint (at {@code /v2/cql}) is disabled by default. */
@QuarkusIntegrationTest
@QuarkusTestResource(StargateTestResource.class)
public class RestApiV2QCqlDisabledIT extends RestApiV2QIntegrationTestBase {
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
