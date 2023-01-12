package io.stargate.sgv2.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import java.util.Arrays;
import java.util.Map;
import org.apache.http.HttpStatus;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@QuarkusTestResource(StargateTestResource.class)
public class RestApiV2QCqlIT extends RestApiV2QIntegrationTestBase {

  public RestApiV2QCqlIT() {
    super("cql_ks_", "cql_t_", KeyspaceCreation.NONE);
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
  public void testCqlQueryRaw() {
    givenWithAuth()
        .contentType(ContentType.TEXT)
        .queryParam("raw", "true")
        .body("SELECT key FROM system.local")
        .when()
        .post(endpointPathForCQL())
        .then()
        .statusCode(200)
        .body("$", Matchers.is(Arrays.asList(Map.of("key", "local"))));
  }

  @Test
  public void testCqlQueryWithKeyspace() {
    givenWithAuth()
        .contentType(ContentType.TEXT)
        .queryParam("keyspace", "system")
        .queryParam("raw", "false")
        .body("SELECT key FROM local")
        .when()
        .post(endpointPathForCQL())
        .then()
        .statusCode(200)
        .body("count", Matchers.is(1))
        .body("data", Matchers.hasSize(1))
        .body("data[0].key", Matchers.is("local"));
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

  @Test
  public void testBadKeyspaceQuery() {
    givenWithAuth()
        .contentType(ContentType.TEXT)
        .body("SELECT key FROM local")
        .when()
        .queryParam("keyspace", "no-such-keyspace")
        .post(endpointPathForCQL())
        .then()
        .statusCode(400)
        .body("code", Matchers.is(HttpStatus.SC_BAD_REQUEST))
        .body(
            "description",
            Matchers.endsWith("INVALID_ARGUMENT: Keyspace 'no-such-keyspace' does not exist"));
  }
}
