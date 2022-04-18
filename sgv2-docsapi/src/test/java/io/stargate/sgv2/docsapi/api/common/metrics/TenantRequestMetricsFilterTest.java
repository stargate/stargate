package io.stargate.sgv2.docsapi.api.common.metrics;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.docsapi.testprofiles.FixedTenantTestProfile;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(TenantRequestMetricsFilterTest.Profile.class)
class TenantRequestMetricsFilterTest {

  public static class Profile extends FixedTenantTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> configOverrides = super.getConfigOverrides();

      return ImmutableMap.<String, String>builder()
          .putAll(configOverrides)
          .put("stargate.metrics.tenant-request-counter.metric-name", "test.metrics")
          .put("stargate.metrics.tenant-request-counter.tenant-tag", "tenantTag")
          .put("stargate.metrics.tenant-request-counter.error-tag", "errorTag")
          .build();
    }
  }

  // simple testing endpoint, helps in testing metrics
  @Path("testing")
  public static class TestingResource {

    @GET
    @Produces("text/plain")
    public String greet() {
      return "hello";
    }
  }

  @Test
  public void record() {
    // call endpoint
    given().when().get("/testing").then().statusCode(200);

    // collect metrics
    String result = given().when().get("/metrics").then().statusCode(200).extract().asString();

    // find target metrics
    List<String> meteredLines =
        Arrays.stream(result.split(System.getProperty("line.separator")))
            .filter(line -> line.startsWith("test_metrics_total"))
            .collect(Collectors.toList());

    assertThat(meteredLines)
        .anySatisfy(
            metric ->
                assertThat(metric)
                    .contains("tenantTag=\"" + FixedTenantTestProfile.TENANT_ID + "\"")
                    .contains("errorTag=\"false\""));
  }
}
