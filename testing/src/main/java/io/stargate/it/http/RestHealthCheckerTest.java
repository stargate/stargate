package io.stargate.it.http;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.it.BaseIntegrationTest;
import java.io.IOException;
import net.jcip.annotations.NotThreadSafe;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

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
    String body = RestUtils.get("", String.format("%s/health", healthUrlBase), HttpStatus.SC_OK);

    assertThat(body).isEqualTo("UP");
  }

  @Test
  public void readiness() throws IOException {
    // Root URL responds like Ping at this point: may change in future
    String body = RestUtils.get("", String.format("%s/", healthUrlBase), HttpStatus.SC_OK);

    assertThat(body).isEqualTo("It's Alive");
  }
}
