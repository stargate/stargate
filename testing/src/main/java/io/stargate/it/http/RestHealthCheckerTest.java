package io.stargate.it.http;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.it.BaseIntegrationTest;
import java.io.IOException;
import java.util.Map;
import net.jcip.annotations.NotThreadSafe;
import org.apache.http.HttpStatus;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@NotThreadSafe
@ExtendWith(RestApiExtension.class)
@RestApiSpec()
public class RestHealthCheckerTest extends BaseIntegrationTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static String healthUrlBase;

  // TODO: what does this do, and is this needed for REST API?
  static {
    System.setProperty("stargate.health_check.data_store.enabled", "true");
    System.setProperty("stargate.health_check.data_store.create_ks_and_table", "true");
  }

  @BeforeAll
  public static void setup(RestApiConnectionInfo restApi) {
    healthUrlBase = "http://" + restApi.host() + ":" + restApi.healthPort();
  }

  @Test
  public void liveness() throws IOException {
    String body =
        RestUtils.get("", String.format("%s/checker/liveness", healthUrlBase), HttpStatus.SC_OK);

    assertThat(body).isEqualTo("UP");
  }

  // TODO what should the parameters be for REST?
  @ParameterizedTest
  @CsvSource({
    ",",
    "?check=deadlocks",
    "?check=graphql",
    "?check=grpc",
    "?check=deadlocks&check=graphql",
    "?check=datastore",
    "?check=storage"
  })
  public void readiness(String query) throws IOException {
    query = query == null ? "" : query;
    String body =
        RestUtils.get(
            "", String.format("%s/checker/readiness%s", healthUrlBase, query), HttpStatus.SC_OK);

    assertThat(body).isEqualTo("READY");
  }

  @Test
  public void missingReadinessCheck() throws IOException {
    String body =
        RestUtils.get(
            "",
            String.format("%s/checker/readiness?check=testUnknown", healthUrlBase),
            HttpStatus.SC_SERVICE_UNAVAILABLE);

    assertThat(body).isEqualTo("NOT READY");
  }

  @Test
  public void healthCheck() throws IOException {
    String body =
        RestUtils.get("", String.format("%s/admin/healthcheck", healthUrlBase), HttpStatus.SC_OK);
    @SuppressWarnings("unchecked")
    Map<String, Object> json = OBJECT_MAPPER.readValue(body, Map.class);
    assertThat(json)
        .extracting("deadlocks", InstanceOfAssertFactories.MAP)
        .containsEntry("healthy", true);
    assertThat(json)
        .extracting("graphql", InstanceOfAssertFactories.MAP)
        .containsEntry("healthy", true)
        .containsEntry("message", "Available");
  }
}
