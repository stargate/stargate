package io.stargate.it.http;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.auth.model.AuthTokenResponse;
import io.stargate.auth.model.Credentials;
import io.stargate.auth.model.Error;
import io.stargate.auth.model.Secret;
import io.stargate.auth.model.UsernameCredentials;
import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.storage.ClusterConnectionInfo;
import java.io.IOException;
import net.jcip.annotations.NotThreadSafe;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NotThreadSafe
public class AuthApiTest extends BaseOsgiIntegrationTest {

  private static final Logger logger = LoggerFactory.getLogger(RestApiv2Test.class);

  private static String host = "http://" + getStargateHost();
  private static final ObjectMapper objectMapper = new ObjectMapper();

  public AuthApiTest(ClusterConnectionInfo backend) {
    super(backend);
  }

  @Test
  public void authTokenGenerate() throws IOException {
    String body =
        RestUtils.post(
            "",
            String.format("%s:8081/v1/auth/token/generate", host),
            objectMapper.writeValueAsString(new Secret("cassandra", "cassandra")),
            HttpStatus.SC_CREATED);

    AuthTokenResponse authTokenResponse = objectMapper.readValue(body, AuthTokenResponse.class);
    assertThat(authTokenResponse.getAuthToken()).isNotNull();
  }

  @Test
  public void authTokenGenerate_BadUsername() throws IOException {
    String body =
        RestUtils.post(
            "",
            String.format("%s:8081/v1/auth/token/generate", host),
            objectMapper.writeValueAsString(new Secret("bad_username", "cassandra")),
            HttpStatus.SC_UNAUTHORIZED);

    Error error = objectMapper.readValue(body, Error.class);
    assertThat(error.getDescription())
        .isEqualTo(
            "Failed to create token: Provided username bad_username and/or password are incorrect");
  }

  @Test
  public void authTokenGenerate_BadPassword() throws IOException {
    String body =
        RestUtils.post(
            "",
            String.format("%s:8081/v1/auth/token/generate", host),
            objectMapper.writeValueAsString(new Secret("cassandra", "bad_password")),
            HttpStatus.SC_UNAUTHORIZED);

    Error error = objectMapper.readValue(body, Error.class);
    assertThat(error.getDescription())
        .isEqualTo(
            "Failed to create token: Provided username cassandra and/or password are incorrect");
  }

  @Test
  public void authTokenGenerate_MissingSecret() throws IOException {
    String body =
        RestUtils.post(
            "",
            String.format("%s:8081/v1/auth/token/generate", host),
            "",
            HttpStatus.SC_BAD_REQUEST);

    Error error = objectMapper.readValue(body, Error.class);
    assertThat(error.getDescription()).isEqualTo("Must provide a body to the request");
  }

  @Test
  public void auth() throws IOException {
    String body =
        RestUtils.post(
            "",
            String.format("%s:8081/v1/auth", host),
            objectMapper.writeValueAsString(new Credentials("cassandra", "cassandra")),
            HttpStatus.SC_CREATED);

    AuthTokenResponse authTokenResponse = objectMapper.readValue(body, AuthTokenResponse.class);
    assertThat(authTokenResponse.getAuthToken()).isNotNull();
  }

  @Test
  public void auth_BadUsername() throws IOException {
    String body =
        RestUtils.post(
            "",
            String.format("%s:8081/v1/auth", host),
            objectMapper.writeValueAsString(new Credentials("bad_username", "cassandra")),
            HttpStatus.SC_UNAUTHORIZED);

    Error error = objectMapper.readValue(body, Error.class);
    assertThat(error.getDescription())
        .isEqualTo(
            "Failed to create token: Provided username bad_username and/or password are incorrect");
  }

  @Test
  public void auth_BadPassword() throws IOException {
    String body =
        RestUtils.post(
            "",
            String.format("%s:8081/v1/auth", host),
            objectMapper.writeValueAsString(new Credentials("cassandra", "bad_password")),
            HttpStatus.SC_UNAUTHORIZED);

    Error error = objectMapper.readValue(body, Error.class);
    assertThat(error.getDescription())
        .isEqualTo(
            "Failed to create token: Provided username cassandra and/or password are incorrect");
  }

  @Test
  public void auth_MissingCredentials() throws IOException {
    String body =
        RestUtils.post("", String.format("%s:8081/v1/auth", host), "", HttpStatus.SC_BAD_REQUEST);

    Error error = objectMapper.readValue(body, Error.class);
    assertThat(error.getDescription()).isEqualTo("Must provide a body to the request");
  }

  @Test
  public void authUsernameToken() throws IOException {
    String body =
        RestUtils.post(
            "",
            String.format("%s:8081/v1/admin/auth/usernametoken", host),
            objectMapper.writeValueAsString(new UsernameCredentials("cassandra")),
            HttpStatus.SC_CREATED);

    AuthTokenResponse authTokenResponse = objectMapper.readValue(body, AuthTokenResponse.class);
    assertThat(authTokenResponse.getAuthToken()).isNotNull();
  }

  @Test
  public void authUsernameToken_BadUsername() throws IOException {
    String body =
        RestUtils.post(
            "",
            String.format("%s:8081/v1/admin/auth/usernametoken", host),
            objectMapper.writeValueAsString(new UsernameCredentials("bad_user_name")),
            HttpStatus.SC_UNAUTHORIZED);

    Error error = objectMapper.readValue(body, Error.class);
    assertThat(error.getDescription())
        .isEqualTo("Failed to create token: Provided username bad_user_name is incorrect");
  }

  @Test
  public void authUsernameToken_MissingUsername() throws IOException {
    String body =
        RestUtils.post(
            "",
            String.format("%s:8081/v1/admin/auth/usernametoken", host),
            "",
            HttpStatus.SC_BAD_REQUEST);

    Error error = objectMapper.readValue(body, Error.class);
    assertThat(error.getDescription()).isEqualTo("Must provide a body to the request");
  }
}
