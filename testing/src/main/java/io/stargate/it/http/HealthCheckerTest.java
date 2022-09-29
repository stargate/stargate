package io.stargate.it.http;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.storage.StargateConnectionInfo;
import java.io.IOException;
import java.util.Map;
import net.jcip.annotations.NotThreadSafe;
import org.apache.http.HttpStatus;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

// Tests for StargateV1 backend health endpoints.
@NotThreadSafe
public class HealthCheckerTest extends BaseIntegrationTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static String host;

  static {
    System.setProperty("stargate.health_check.data_store.enabled", "true");
    System.setProperty("stargate.health_check.data_store.create_ks_and_table", "true");
  }

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

  // TODO: are any of these related to REST?
  @ParameterizedTest
  @CsvSource({
    ",",
    "?check=deadlocks",
    "?check=grpc",
    "?check=datastore",
    "?check=storage"
  })
  public void readiness(String query) throws IOException {
    query = query == null ? "" : query;
    String body =
        RestUtils.get(
            "", String.format("%s:8084/checker/readiness%s", host, query), HttpStatus.SC_OK);

    assertThat(body).isEqualTo("READY");
  }

  @Test
  public void missingReadinessCheck() throws IOException {
    String body =
        RestUtils.get(
            "",
            String.format("%s:8084/checker/readiness?check=testUnknown", host),
            HttpStatus.SC_SERVICE_UNAVAILABLE);

    assertThat(body).isEqualTo("NOT READY");
  }

  @Test
  public void healthCheck() throws IOException {
    String body =
        RestUtils.get("", String.format("%s:8084/admin/healthcheck", host), HttpStatus.SC_OK);
    @SuppressWarnings("unchecked")
    Map<String, Object> json = OBJECT_MAPPER.readValue(body, Map.class);
    assertThat(json)
        .extracting("deadlocks", InstanceOfAssertFactories.MAP)
        .containsEntry("healthy", true);
  }
}
