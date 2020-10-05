package io.stargate.it.cql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.fail;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.auth.model.AuthTokenResponse;
import io.stargate.it.http.RestUtils;
import io.stargate.it.http.models.Credentials;
import io.stargate.it.storage.ClusterConnectionInfo;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

// TODO find a workaround to reenable this test
@Disabled(
    "Needs to run in isolation because it relies on a static variable set from a system property")
public class AuthenticationTest extends JavaDriverTestBase {

  public AuthenticationTest(ClusterConnectionInfo backend) {
    super(backend);
    enableAuth = true;
  }

  @BeforeAll
  public static void beforeAll() {
    System.setProperty("stargate.cql_use_auth_service", "true");
  }

  @AfterAll
  public static void afterAll() {
    // Note that this assumes that tests runs serially
    System.setProperty("stargate.cql_use_auth_service", "false");
  }

  @Test
  @DisplayName("Should fail to connect if auth credentials are invalid")
  public void invalidCredentials() {
    assertThatThrownBy(() -> newSessionBuilder().withAuthCredentials("invalid", "invalid").build())
        .isInstanceOf(AllNodesFailedException.class)
        .hasMessageContaining("Provided username invalid and/or password are incorrect");
  }

  @Test
  @DisplayName("Should connect with auth token")
  public void tokenAuthentication() {
    try (CqlSession tokenSession =
        newSessionBuilder().withAuthCredentials("token", getAuthToken()).build()) {
      Row row = tokenSession.execute("SELECT * FROM system.local").one();
      assertThat(row).isNotNull();
    }
  }

  private String getAuthToken() {
    try {

      ObjectMapper objectMapper = new ObjectMapper();
      objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

      String body =
          RestUtils.post(
              "",
              String.format("%s:8081/v1/auth/token/generate", "http://" + getStargateHost()),
              objectMapper.writeValueAsString(new Credentials("cassandra", "cassandra")),
              HttpStatus.SC_CREATED);

      AuthTokenResponse authTokenResponse = objectMapper.readValue(body, AuthTokenResponse.class);
      String authToken = authTokenResponse.getAuthToken();
      assertThat(authToken).isNotNull();
      return authToken;
    } catch (Exception e) {
      fail("Unexpected error while fetching auth token", e);
      throw new AssertionError(); // never reached
    }
  }
}
