package io.stargate.it.http;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import net.jcip.annotations.NotThreadSafe;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@NotThreadSafe
@ExtendWith(ApiServiceExtension.class)
@ApiServiceSpec(parametersCustomizer = "buildApiServiceParameters")
public class RestHealthCheckerTest extends BaseRestApiTest {
  private static String healthUrlBase;

  @BeforeAll
  public static void setup(ApiServiceConnectionInfo restApi) {
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
