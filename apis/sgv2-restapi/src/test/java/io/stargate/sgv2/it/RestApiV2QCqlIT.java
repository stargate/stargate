package io.stargate.sgv2.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import org.junit.jupiter.api.Test;

/** Integration tests for CQL endpoint (at {@code /v2/cql}). */
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
            .body("SELECT key FROM system.local")
            .when()
            .post(endpointPathForCQL())
            .then()
            .statusCode(200)
            .extract()
            .asString();

    assertThat(r).isEqualTo("{\"count\":1,\"data\":[{\"key\":\"local\"}]}");
  }

  @Test
  public void testBadCqlQuery() {

    String r =
        givenWithAuth()
            .contentType(ContentType.TEXT)
            .body("SELsECT fo FROM sjkakk")
            .when()
            .post(endpointPathForCQL())
            .then()
            .statusCode(400)
            .extract()
            .asString();

    assertThat(r)
        .isEqualTo(
            "{\"description\":\"Invalid argument for gRPC operation (Status.Code.INVALID_ARGUMENT->400): INVALID_ARGUMENT: line 1:0 no viable alternative at input 'SELsECT' ([SELsECT]...)\",\"code\":400,\"grpcStatus\":\"INVALID_ARGUMENT\",\"internalTxId\":null}");
  }

  @Test
  public void testMissingCqlQuery() {

    String r =
        givenWithAuth()
            .contentType(ContentType.TEXT)
            .when()
            .post(endpointPathForCQL())
            .then()
            .statusCode(400)
            .extract()
            .asString();

    assertThat(r)
        .isEqualTo(
            "{\"description\":\"Request invalid: CQL query body required.\",\"code\":400,\"grpcStatus\":null,\"internalTxId\":null}");
  }
}
