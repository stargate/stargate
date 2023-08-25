package io.stargate.sgv2.it.aux;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestInstance;

@QuarkusIntegrationTest
@QuarkusTestResource(StargateTestResource.class)
@TestClassOrder(ClassOrderer.DisplayName.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RestApiV2QHealthCheckerIT {
  protected static final ObjectMapper OBJECT_MAPPER = JsonMapper.builder().build();

  // For /stargate/health
  @Test
  public void checkHealthMain() throws Exception {
    String response =
        given()
            .when()
            .get("/stargate/health")
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    JsonNode json = OBJECT_MAPPER.readTree(response);
    assertThat(json.at("/status").asText()).isEqualTo("UP");
  }

  // For /stargate/health/live - The application is up and running.
  @Test
  public void checkHealthLive() throws Exception {
    String response =
        given()
            .when()
            .get("/stargate/health/live")
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    JsonNode json = OBJECT_MAPPER.readTree(response);
    assertThat(json.at("/status").asText()).isEqualTo("UP");
  }

  // We also have legacy health-point added and used by routing logic
  @Test
  public void checkLegacyHealth() {
    String response =
        given().when().get("/health").then().statusCode(HttpStatus.SC_OK).extract().asString();
    assertThat(response).startsWith("UP");
  }

  @Test
  public void checkLegacyPing() {
    String response =
        given().when().get("/ping").then().statusCode(HttpStatus.SC_OK).extract().asString();
    assertThat(response).startsWith("It's Alive");
  }
}
