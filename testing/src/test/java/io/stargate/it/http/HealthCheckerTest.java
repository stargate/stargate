package io.stargate.it.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.storage.ClusterConnectionInfo;
import java.io.IOException;
import net.jcip.annotations.NotThreadSafe;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NotThreadSafe
public class HealthCheckerTest extends BaseOsgiIntegrationTest {

  private static final Logger logger = LoggerFactory.getLogger(HealthCheckerTest.class);

  private static String host = "http://" + getStargateHost();

  public HealthCheckerTest(ClusterConnectionInfo backend) {
    super(backend);
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
}
