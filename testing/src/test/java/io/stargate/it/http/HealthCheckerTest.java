package io.stargate.it.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.storage.StargateConnectionInfo;
import java.io.IOException;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Execution(ExecutionMode.CONCURRENT)
public class HealthCheckerTest extends BaseOsgiIntegrationTest {

  private static final Logger logger = LoggerFactory.getLogger(HealthCheckerTest.class);

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
}
