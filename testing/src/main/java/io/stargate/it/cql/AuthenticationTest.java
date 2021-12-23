package io.stargate.it.cql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.fail;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.auth.model.AuthTokenResponse;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.http.RestUtils;
import io.stargate.it.http.models.Credentials;
import io.stargate.it.storage.StargateConnectionInfo;
import io.stargate.it.storage.StargateParameters;
import io.stargate.it.storage.StargateSpec;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@StargateSpec(parametersCustomizer = "buildParameters")
@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(createSession = false)
public class AuthenticationTest extends BaseIntegrationTest {

  private StargateConnectionInfo stargate;

  @SuppressWarnings("unused") // referenced in @StargateSpec
  public static void buildParameters(StargateParameters.Builder builder) {
    builder.enableAuth(true);
    builder.putSystemProperties("stargate.cql_use_auth_service", "true");
  }

  @BeforeEach
  public void init(StargateConnectionInfo stargate) {
    this.stargate = stargate;
  }

  @Test
  @DisplayName("Should fail to connect if auth credentials are invalid")
  public void invalidCredentials(CqlSessionBuilder sessionBuilder) {
    assertThatThrownBy(() -> sessionBuilder.withAuthCredentials("invalid", "invalid").build())
        .isInstanceOf(AllNodesFailedException.class)
        .hasMessageContaining("Provided username invalid and/or password are incorrect");
  }

  @Test
  @DisplayName("Should connect with auth token")
  public void tokenAuthentication(CqlSessionBuilder sessionBuilder) {
    try (CqlSession tokenSession =
        sessionBuilder.withAuthCredentials("token", getAuthToken()).build()) {
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
              String.format("%s:8081/v1/auth/token/generate", "http://" + stargate.seedAddress()),
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
