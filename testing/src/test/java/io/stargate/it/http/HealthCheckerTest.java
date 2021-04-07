package io.stargate.it.http;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.storage.StargateConnectionInfo;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import net.jcip.annotations.NotThreadSafe;
import org.apache.http.HttpStatus;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

@NotThreadSafe
public class HealthCheckerTest extends BaseOsgiIntegrationTest {

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

  @ParameterizedTest
  @CsvSource({
    ",",
    "?check=deadlocks",
    "?check=graphql",
    "?check=deadlocks&check=graphql",
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
    assertThat(json)
        .extracting("graphql", InstanceOfAssertFactories.MAP)
        .containsEntry("healthy", true)
        .containsEntry("message", "Ready to process requests");
  }

  @ParameterizedTest
  @ValueSource(strings = {"authapi", "graphqlapi", "health_checker", "restapi"})
  public void metricsModule(String module) throws IOException {
    String[] expectedMetricGroups =
        new String[] {"TimeBoundHealthCheck", "io_dropwizard_jersey", "org_eclipse_jetty"};

    String result = RestUtils.get("", String.format("%s:8084/metrics", host), HttpStatus.SC_OK);

    String[] lines = result.split(System.getProperty("line.separator"));
    long foundMetricGroups =
        Arrays.stream(expectedMetricGroups)
            .map(
                metricGroup ->
                    Arrays.stream(lines)
                        .anyMatch(line -> line.startsWith(module + "_" + metricGroup)))
            .filter(Boolean::booleanValue)
            .count();

    assertThat(foundMetricGroups).isEqualTo(expectedMetricGroups.length);
  }

  @Test
  public void metricsPersistence() throws IOException {
    String version = backend.clusterVersion().replace('.', '_');
    String expectedPrefix =
        (backend.isDse() ? "persistence_dse" : "persistence_cassandra") + "_" + version;

    String result = RestUtils.get("", String.format("%s:8084/metrics", host), HttpStatus.SC_OK);

    String[] lines = result.split(System.getProperty("line.separator"));
    boolean prefixFound = Arrays.stream(lines).anyMatch(line -> line.startsWith(expectedPrefix));

    assertThat(prefixFound).isTrue();
  }
}
