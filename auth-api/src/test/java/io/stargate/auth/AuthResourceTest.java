package io.stargate.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import io.stargate.auth.api.resources.AuthResource;
import io.stargate.auth.model.AuthApiError;
import io.stargate.auth.model.AuthTokenResponse;
import io.stargate.auth.model.Credentials;
import io.stargate.auth.model.Secret;
import io.stargate.auth.model.UsernameCredentials;
import java.util.Collections;
import java.util.Map;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(DropwizardExtensionsSupport.class)
class AuthResourceTest {

  private static final AuthenticationService authService = mock(AuthenticationService.class);

  private static final ResourceExtension resourceWithUsernameTokenDisabled =
      ResourceExtension.builder().addResource(new AuthResource(authService, false)).build();

  private static final ResourceExtension resourceWithUsernameTokenEnabled =
      ResourceExtension.builder().addResource(new AuthResource(authService, true)).build();

  private static final Map<String, String> EMPTY_HEADERS = Collections.emptyMap();

  @AfterEach
  void tearDown() {
    reset(authService);
  }

  @Test
  void createTokenFromSecretSuccess() throws UnauthorizedException {
    Secret secret = new Secret("key", "secret");
    when(authService.createToken("key", "secret", EMPTY_HEADERS)).thenReturn("token");

    AuthTokenResponse token =
        resourceWithUsernameTokenDisabled
            .target("/v1/auth/token/generate")
            .request()
            .post(Entity.entity(secret, MediaType.APPLICATION_JSON), AuthTokenResponse.class);

    assertThat(token.getAuthToken()).isEqualTo("token");
  }

  @Test
  void createTokenFromSecretUnauthorized() throws UnauthorizedException {
    Secret secret = new Secret("key", "secret");
    when(authService.createToken("key", "secret", EMPTY_HEADERS))
        .thenThrow(new UnauthorizedException("unauthorized"));

    Response response =
        resourceWithUsernameTokenDisabled
            .target("/v1/auth/token/generate")
            .request()
            .post(Entity.entity(secret, MediaType.APPLICATION_JSON));

    assertThat(response.getStatusInfo().getStatusCode()).isEqualTo(401);
    assertThat(response.readEntity(AuthApiError.class).getDescription())
        .isEqualTo("Failed to create token: unauthorized");
  }

  @Test
  void createTokenFromSecretInternalServerError() throws UnauthorizedException {
    Secret secret = new Secret("key", "secret");
    when(authService.createToken("key", "secret", EMPTY_HEADERS))
        .thenThrow(new RuntimeException("error"));

    Response response =
        resourceWithUsernameTokenDisabled
            .target("/v1/auth/token/generate")
            .request()
            .post(Entity.entity(secret, MediaType.APPLICATION_JSON));

    assertThat(response.getStatusInfo().getStatusCode()).isEqualTo(500);
    assertThat(response.readEntity(AuthApiError.class).getDescription())
        .isEqualTo("Failed to create token: error");
  }

  @Test
  void createTokenFromSecretNoPayload() {
    Response response =
        resourceWithUsernameTokenDisabled.target("/v1/auth/token/generate").request().post(null);

    assertThat(response.getStatusInfo().getStatusCode()).isEqualTo(400);
    assertThat(response.readEntity(AuthApiError.class).getDescription())
        .isEqualTo("Must provide a body to the request");
  }

  @Test
  void createTokenFromSecretEmptyKey() {
    Secret secret = new Secret("", "secret");

    Response response =
        resourceWithUsernameTokenDisabled
            .target("/v1/auth/token/generate")
            .request()
            .post(Entity.entity(secret, MediaType.APPLICATION_JSON));

    assertThat(response.getStatusInfo().getStatusCode()).isEqualTo(400);
    assertThat(response.readEntity(AuthApiError.class).getDescription())
        .isEqualTo("Must provide key in request");
  }

  @Test
  void createTokenFromSecretEmptySecret() {
    Secret secret = new Secret("key", "");

    Response response =
        resourceWithUsernameTokenDisabled
            .target("/v1/auth/token/generate")
            .request()
            .post(Entity.entity(secret, MediaType.APPLICATION_JSON));

    assertThat(response.getStatusInfo().getStatusCode()).isEqualTo(400);
    assertThat(response.readEntity(AuthApiError.class).getDescription())
        .isEqualTo("Must provide secret in request");
  }

  @Test
  void createTokenFromCredentialsSuccess() throws UnauthorizedException {
    Credentials credentials = new Credentials("username", "password");
    when(authService.createToken("username", "password", EMPTY_HEADERS)).thenReturn("token");

    AuthTokenResponse token =
        resourceWithUsernameTokenDisabled
            .target("/v1/auth")
            .request()
            .post(Entity.entity(credentials, MediaType.APPLICATION_JSON), AuthTokenResponse.class);

    assertThat(token.getAuthToken()).isEqualTo("token");
  }

  @Test
  void createTokenFromCredentialsUnauthorized() throws UnauthorizedException {
    Credentials credentials = new Credentials("username", "password");
    when(authService.createToken("username", "password", EMPTY_HEADERS))
        .thenThrow(new UnauthorizedException("unauthorized"));

    Response response =
        resourceWithUsernameTokenDisabled
            .target("/v1/auth")
            .request()
            .post(Entity.entity(credentials, MediaType.APPLICATION_JSON));

    assertThat(response.getStatusInfo().getStatusCode()).isEqualTo(401);
    assertThat(response.readEntity(AuthApiError.class).getDescription())
        .isEqualTo("Failed to create token: unauthorized");
  }

  @Test
  void createTokenFromCredentialsInternalServerError() throws UnauthorizedException {
    Credentials credentials = new Credentials("username", "password");
    when(authService.createToken("username", "password", EMPTY_HEADERS))
        .thenThrow(new RuntimeException("error"));

    Response response =
        resourceWithUsernameTokenDisabled
            .target("/v1/auth")
            .request()
            .post(Entity.entity(credentials, MediaType.APPLICATION_JSON));

    assertThat(response.getStatusInfo().getStatusCode()).isEqualTo(500);
    assertThat(response.readEntity(AuthApiError.class).getDescription())
        .isEqualTo("Failed to create token: error");
  }

  @Test
  void createTokenFromCredentialsNoPayload() {
    Response response = resourceWithUsernameTokenDisabled.target("/v1/auth").request().post(null);

    assertThat(response.getStatusInfo().getStatusCode()).isEqualTo(400);
    assertThat(response.readEntity(AuthApiError.class).getDescription())
        .isEqualTo("Must provide a body to the request");
  }

  @Test
  void createTokenFromCredentialsEmptyUsername() {
    Credentials credentials = new Credentials("", "password");

    Response response =
        resourceWithUsernameTokenDisabled
            .target("/v1/auth")
            .request()
            .post(Entity.entity(credentials, MediaType.APPLICATION_JSON));

    assertThat(response.getStatusInfo().getStatusCode()).isEqualTo(400);
    assertThat(response.readEntity(AuthApiError.class).getDescription())
        .isEqualTo("Must provide username in request");
  }

  @Test
  void createTokenFromCredentialsEmptyPassword() {
    Credentials credentials = new Credentials("username", "");

    Response response =
        resourceWithUsernameTokenDisabled
            .target("/v1/auth")
            .request()
            .post(Entity.entity(credentials, MediaType.APPLICATION_JSON));

    assertThat(response.getStatusInfo().getStatusCode()).isEqualTo(400);
    assertThat(response.readEntity(AuthApiError.class).getDescription())
        .isEqualTo("Must provide password in request");
  }

  @Test
  void createTokenFromUsernameSuccess() throws UnauthorizedException {
    UsernameCredentials username = new UsernameCredentials("username");
    when(authService.createToken("username", EMPTY_HEADERS)).thenReturn("token");

    AuthTokenResponse token =
        resourceWithUsernameTokenEnabled
            .target("/v1/admin/auth/usernametoken")
            .request()
            .post(Entity.entity(username, MediaType.APPLICATION_JSON), AuthTokenResponse.class);

    assertThat(token.getAuthToken()).isEqualTo("token");
  }

  @Test
  void createTokenFromUsernameDisabled() throws UnauthorizedException {
    UsernameCredentials username = new UsernameCredentials("username");
    when(authService.createToken("username", EMPTY_HEADERS)).thenReturn("token");

    Response response =
        resourceWithUsernameTokenDisabled
            .target("/v1/admin/auth/usernametoken")
            .request()
            .post(Entity.entity(username, MediaType.APPLICATION_JSON));

    assertThat(response.getStatusInfo().getStatusCode()).isEqualTo(400);
    assertThat(response.readEntity(AuthApiError.class).getDescription())
        .isEqualTo("Generating a token for a username is not allowed");
  }

  @Test
  void createTokenFromUsernameUnauthorized() throws UnauthorizedException {
    UsernameCredentials username = new UsernameCredentials("username");
    when(authService.createToken("username", EMPTY_HEADERS))
        .thenThrow(new UnauthorizedException("unauthorized"));

    Response response =
        resourceWithUsernameTokenEnabled
            .target("/v1/admin/auth/usernametoken")
            .request()
            .post(Entity.entity(username, MediaType.APPLICATION_JSON));

    assertThat(response.getStatusInfo().getStatusCode()).isEqualTo(401);
    assertThat(response.readEntity(AuthApiError.class).getDescription())
        .isEqualTo("Failed to create token: unauthorized");
  }

  @Test
  void createTokenFromUsernameInternalServerError() throws UnauthorizedException {
    UsernameCredentials username = new UsernameCredentials("username");
    when(authService.createToken("username", EMPTY_HEADERS))
        .thenThrow(new RuntimeException("error"));

    Response response =
        resourceWithUsernameTokenEnabled
            .target("/v1/admin/auth/usernametoken")
            .request()
            .post(Entity.entity(username, MediaType.APPLICATION_JSON));

    assertThat(response.getStatusInfo().getStatusCode()).isEqualTo(500);
    assertThat(response.readEntity(AuthApiError.class).getDescription())
        .isEqualTo("Failed to create token: error");
  }

  @Test
  void createTokenFromUsernameNoPayload() {
    Response response =
        resourceWithUsernameTokenEnabled
            .target("/v1/admin/auth/usernametoken")
            .request()
            .post(null);

    assertThat(response.getStatusInfo().getStatusCode()).isEqualTo(400);
    assertThat(response.readEntity(AuthApiError.class).getDescription())
        .isEqualTo("Must provide a body to the request");
  }

  @Test
  void createTokenFromUsernameEmptyUsername() {
    UsernameCredentials username = new UsernameCredentials("");

    Response response =
        resourceWithUsernameTokenEnabled
            .target("/v1/admin/auth/usernametoken")
            .request()
            .post(Entity.entity(username, MediaType.APPLICATION_JSON));

    assertThat(response.getStatusInfo().getStatusCode()).isEqualTo(400);
    assertThat(response.readEntity(AuthApiError.class).getDescription())
        .isEqualTo("Must provide username in request");
  }
}
