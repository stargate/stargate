package io.stargate.sgv2.it;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.common.testprofiles.IntegrationTestProfile;
import io.stargate.sgv2.restapi.service.models.Sgv2Keyspace;
import java.io.IOException;
import javax.enterprise.context.control.ActivateRequestContext;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestInstance;

@QuarkusTest
@TestProfile(IntegrationTestProfile.class)
@ActivateRequestContext
@TestClassOrder(ClassOrderer.ClassName.class) // prefer stable even if arbitrary ordering
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RestApiV2QSchemaTest extends RestApiV2QIntegrationTestBase {

  @Test
  public void keyspacesGetAll() throws IOException {
    String response =
        given()
            .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
            .when()
            .get("/v2/schemas/keyspaces")
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    assertSystemKeyspaces(readWrappedRESTResponse(response, Sgv2Keyspace[].class));
  }

  @Test
  public void keyspacesGetAllRaw() throws IOException {
    String response =
        given()
            .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, "")
            .queryParam("raw", "true")
            .when()
            .get("/v2/schemas/keyspaces")
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    assertSystemKeyspaces(readJsonAs(response, Sgv2Keyspace[].class));
  }

  private void assertSystemKeyspaces(Sgv2Keyspace[] keyspaces) {
    assertSimpleKeyspaces(keyspaces, "system", "system_auth", "system_schema");
  }

  private void assertSimpleKeyspaces(Sgv2Keyspace[] keyspaces, String... expectedKsNames) {
    for (String ksName : expectedKsNames) {
      assertThat(keyspaces)
          .anySatisfy(
              v -> assertThat(v).usingRecursiveComparison().isEqualTo(new Sgv2Keyspace(ksName)));
    }
  }
}
