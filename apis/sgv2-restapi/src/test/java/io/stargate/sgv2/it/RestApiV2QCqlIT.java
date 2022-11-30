package io.stargate.sgv2.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@QuarkusTestResource(StargateTestResource.class)
public class RestApiV2QCqlIT extends RestApiV2QIntegrationTestBase {

  public RestApiV2QCqlIT() {
    super("cql_ks_", "cql_t_");
  }

  @Test
  public void testCqlQuery() {

    String r =
        givenWithAuth()
            .contentType(ContentType.TEXT)
            .body("SELECT cluster_name FROM system.local")
            .when()
            .post(endpointPathForCQL())
            .then()
            .statusCode(200)
            .extract()
            .asString();

    assertThat(r).isEqualTo("{\"count\":1,\"data\":[{\"cluster_name\":\"int-test-cluster\"}]}");
  }
}
