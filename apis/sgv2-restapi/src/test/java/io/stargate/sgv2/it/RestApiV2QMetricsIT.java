package io.stargate.sgv2.it;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import java.util.List;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestInstance;

@QuarkusIntegrationTest
@QuarkusTestResource(StargateTestResource.class)
@TestClassOrder(ClassOrderer.DisplayName.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RestApiV2QMetricsIT {
  // For /metrics (Prometheus endpoint)
  @Test
  public void checkMetricsPrometheus() throws Exception {
    String response =
        given().when().get("/metrics").then().statusCode(HttpStatus.SC_OK).extract().asString();
    List<String> line = response.lines().toList();
    // Initial trivial test to see there is an endpoint. To improve:
    //
    // 1. Trim comment lines
    // 2. Run as full integration test to produce call entries, verify
    //    that we have 'module="sgv2-restapi"'
    //
    assertThat(line).hasSizeGreaterThan(10);
  }
}
