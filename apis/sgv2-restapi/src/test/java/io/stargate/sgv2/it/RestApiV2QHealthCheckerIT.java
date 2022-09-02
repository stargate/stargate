package io.stargate.sgv2.it;

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

  // For /q/health
  @Test
  public void checkHealthMain() throws Exception {
    String response =
        given().when().get("/q/health").then().statusCode(HttpStatus.SC_OK).extract().asString();
    JsonNode json = OBJECT_MAPPER.readTree(response);
    assertThat(json.at("/status").asText()).isEqualTo("UP");
  }

  // For /q/health/live - The application is up and running.
  @Test
  public void checkHealthLive() throws Exception {
    String response =
        given()
            .when()
            .get("/q/health/live")
            .then()
            .statusCode(HttpStatus.SC_OK)
            .extract()
            .asString();
    JsonNode json = OBJECT_MAPPER.readTree(response);
    assertThat(json.at("/status").asText()).isEqualTo("UP");
  }
}
