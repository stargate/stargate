package io.stargate.it.http;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.storage.StargateConnectionInfo;
import java.io.IOException;
import java.util.Map;
import net.jcip.annotations.NotThreadSafe;
import org.apache.http.HttpStatus;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@NotThreadSafe
public class HealthCheckerTest extends BaseOsgiIntegrationTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static String host;

  @BeforeAll
  public static void setup(StargateConnectionInfo cluster) {
    host = "http://" + cluster.seedAddress();
  }

  // TODO: Add further test cases by bringing persistence up and down along with modifying running
  // stargate modules

  @Test
  public void liveness() throws IOException {
    String body =
        RestUtils.get("", String.format("%s:8084/checker/liveness", host), HttpStatus.SC_OK);

    assertThat(body).isEqualTo("UP");
  }

  @Test
  public void readiness() throws IOException {
    String body =
        RestUtils.get("", String.format("%s:8084/checker/readiness", host), HttpStatus.SC_OK);

    assertThat(body).isEqualTo("READY");
  }

  @Test
  public void healthCheck() throws IOException {
    String body = RestUtils.get("", String.format("%s:8084/healthcheck", host), HttpStatus.SC_OK);
    @SuppressWarnings("unchecked")
    Map<String, Object> json = OBJECT_MAPPER.readValue(body, Map.class);
    assertThat(json)
        .extracting("deadlocks", InstanceOfAssertFactories.MAP)
        .containsEntry("healthy", true);
    assertThat(json)
        .extracting("graphql", InstanceOfAssertFactories.MAP)
        .containsEntry("healthy", true)
        .containsEntry("message", "Ready to process requests");
  }
}
