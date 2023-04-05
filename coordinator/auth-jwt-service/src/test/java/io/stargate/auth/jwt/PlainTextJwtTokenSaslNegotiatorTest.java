package io.stargate.auth.jwt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.Credentials;
import io.stargate.auth.PlainTextTokenSaslNegotiator;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.AuthenticatedUser;
import io.stargate.db.Authenticator.SaslNegotiator;
import io.stargate.db.ClientInfo;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import org.apache.cassandra.stargate.exceptions.AuthenticationException;
import org.junit.jupiter.api.Test;

class PlainTextJwtTokenSaslNegotiatorTest {

  private final ClientInfo clientInfo =
      new ClientInfo(
          InetSocketAddress.createUnresolved("localhost", 1234),
          InetSocketAddress.createUnresolved("localhost", 4321));

  private final String TOKEN =
      "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJZTm9oTElUUjZMOHhT"
          + "NC1iVnp6TEhCdTY3NTRranc4QnFBZ2Nmc2hoUGk4In0.eyJleHAiOjE2MDU1OTk1ODMsImlhdCI6MTYwNTU5ODk4Myw"
          + "ianRpIjoiMzQ0Y2M1MTItMjk2Ni00MjU4LTlkYjEtYjYwN2JkODA2YmVhIiwiaXNzIjoiaHR0cDovL2xvY2FsaG9zdD"
          + "o0NDQ0L2F1dGgvcmVhbG1zL3N0YXJnYXRlIiwiYXVkIjoiYWNjb3VudCIsInN1YiI6ImI2Yjk2MjkxLTBmNWUtNDk4N"
          + "y05YjhjLTUxOWIyOTk4YzZmYSIsInR5cCI6IkJlYXJlciIsImF6cCI6InVzZXItc2VydmljZSIsInNlc3Npb25fc3Rh"
          + "dGUiOiJhYmIzYjAyYS1mMGY3LTQxOWYtYjI4Yy1hNTkzZjA5ZTkwYzUiLCJhY3IiOiIxIiwicmVhbG1fYWNjZXNzIjp"
          + "7InJvbGVzIjpbIm9mZmxpbmVfYWNjZXNzIiwidW1hX2F1dGhvcml6YXRpb24iXX0sInJlc291cmNlX2FjY2VzcyI6ey"
          + "JhY2NvdW50Ijp7InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb"
          + "2ZpbGUiXX19LCJzY29wZSI6InByb2ZpbGUgZW1haWwiLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwic3RhcmdhdGVfY2xh"
          + "aW1zIjp7Ingtc3RhcmdhdGUtcm9sZSI6IndlYl91c2VyIiwieC1zdGFyZ2F0ZS11c2VyaWQiOiI5ODc2In0sInByZWZ"
          + "lcnJlZF91c2VybmFtZSI6InRlc3R1c2VyMSJ9.L3mYuCNbMfTeqG79oWJTG5aRXPiGWNVr4KugW_xnym7NCdFfZF8Ot"
          + "ck9hT5ArRGkichibYpQAxv323CDcFs17CezuqjOrkgw1LiuiZBzkF4780FnHN54oRd7AcZ9_h9ahWvTwfV1ao2_mgZ0"
          + "XRS8jonqhZxjcgFlhYD0B4Sg8T6UVkDdp63EYoBJfZjhPfKcvmTxlT-Ysi_sFjeYKq9wOsTXQ9hZQRnqWUPgrU03-Rr"
          + "WHk6Vm77CxS0KpVmw54D1MVg50KBACs0jzz4KhDDP0J10ss2lTS1PDpCMvk3L2id2v_SdS7yo3Ci2SND-Nah6KqnCRy"
          + "lHcv3orB4UDkaD8A";
  private final String ROLE = "someRole";
  private final int TOKEN_MAX_LENGTH = 4096;
  private final String TOKEN_USERNAME = "token";

  @Test
  public void decodeCredentials() {
    Credentials credentials =
        PlainTextJwtTokenSaslNegotiator.decodeCredentials(
            new byte[] {0, 97, 98, 99, 0, 97, 98, 99});
    assertThat(credentials.getUsername()).isEqualTo("abc");
    assertThat(credentials.getPassword()).isEqualTo(new char[] {'a', 'b', 'c'});
  }

  @Test
  public void invalidDecodeCredentials() {
    // Empty
    assertThatThrownBy(() -> PlainTextJwtTokenSaslNegotiator.decodeCredentials(new byte[] {}))
        .isInstanceOf(AuthenticationException.class)
        .hasMessage("Password must not be null");

    // Empty authzid
    assertThatThrownBy(() -> PlainTextJwtTokenSaslNegotiator.decodeCredentials(new byte[] {0}))
        .isInstanceOf(AuthenticationException.class)
        .hasMessage("Password must not be null");

    // Empty authzid and authnid
    assertThatThrownBy(() -> PlainTextJwtTokenSaslNegotiator.decodeCredentials(new byte[] {0, 0}))
        .isInstanceOf(AuthenticationException.class)
        .hasMessage("Password must not be null");

    // Only authzid
    assertThatThrownBy(
            () -> PlainTextJwtTokenSaslNegotiator.decodeCredentials(new byte[] {97, 98, 99, 0, 0}))
        .isInstanceOf(AuthenticationException.class)
        .hasMessage("Password must not be null");

    // Empty authzid, but valid authnid and password
    assertThatThrownBy(
            () -> PlainTextJwtTokenSaslNegotiator.decodeCredentials(new byte[] {0, 97, 0, 97, 0}))
        .isInstanceOf(AuthenticationException.class)
        .hasMessage(
            "Credential format error: username or password is empty or contains NUL(\\0) character");

    // Non-empty authzid
    assertThatThrownBy(
            () ->
                PlainTextJwtTokenSaslNegotiator.decodeCredentials(new byte[] {97, 0, 97, 0, 97, 0}))
        .isInstanceOf(AuthenticationException.class)
        .hasMessage(
            "Credential format error: username or password is empty or contains NUL(\\0) character");
  }

  @Test
  public void useToken() throws IOException, UnauthorizedException {
    final byte[] clientResponse = createClientResponse(TOKEN_USERNAME, TOKEN);

    AuthenticationService authentication = mock(AuthenticationService.class);
    when(authentication.validateToken(TOKEN, clientInfo))
        .thenReturn(AuthenticationSubject.of(TOKEN, ROLE));

    PlainTextTokenSaslNegotiator negotiator =
        new PlainTextJwtTokenSaslNegotiator(
            authentication, null, TOKEN_USERNAME, TOKEN_MAX_LENGTH, clientInfo);
    assertThat(negotiator.evaluateResponse(clientResponse)).isNull();
    assertThat(negotiator.isComplete()).isTrue();
    assertThat(negotiator.getAuthenticatedUser().name()).isEqualTo(ROLE);
  }

  @Test
  public void useWrapped() throws IOException {
    final byte[] clientResponse = createClientResponse("user", "pass");

    SaslNegotiator wrappedNegotiator = mock(SaslNegotiator.class);
    when(wrappedNegotiator.evaluateResponse(clientResponse)).thenReturn(null);
    when(wrappedNegotiator.isComplete()).thenReturn(true);
    doReturn(AuthenticatedUser.of(ROLE)).when(wrappedNegotiator).getAuthenticatedUser();

    PlainTextTokenSaslNegotiator negotiator =
        new PlainTextJwtTokenSaslNegotiator(
            null, wrappedNegotiator, TOKEN_USERNAME, TOKEN_MAX_LENGTH, clientInfo);
    assertThat(negotiator.evaluateResponse(clientResponse)).isNull();
    assertThat(negotiator.isComplete()).isTrue();
    assertThat(negotiator.getAuthenticatedUser().name()).isEqualTo(ROLE);
  }

  @Test
  public void tokenGreaterThanMaxLength() throws IOException {
    final String tooLongToken = new String(new char[TOKEN_MAX_LENGTH + 1]).replace('\0', 'a');

    SaslNegotiator wrappedNegotiator = mock(SaslNegotiator.class);
    when(wrappedNegotiator.isComplete()).thenReturn(false);

    PlainTextTokenSaslNegotiator negotiator =
        new PlainTextJwtTokenSaslNegotiator(
            null, wrappedNegotiator, TOKEN_USERNAME, TOKEN_MAX_LENGTH, clientInfo);
    assertThat(
            negotiator.attemptTokenAuthentication(
                createClientResponse(TOKEN_USERNAME, tooLongToken)))
        .isFalse();
    assertThat(negotiator.isComplete()).isFalse();
  }

  @Test
  public void authServiceReturnsNullCredentials() throws UnauthorizedException, IOException {
    AuthenticationService authentication = mock(AuthenticationService.class);
    when(authentication.validateToken(TOKEN, clientInfo)).thenReturn(null);

    SaslNegotiator wrappedNegotiator = mock(SaslNegotiator.class);
    when(wrappedNegotiator.isComplete()).thenReturn(false);

    PlainTextTokenSaslNegotiator negotiator =
        new PlainTextJwtTokenSaslNegotiator(
            authentication, wrappedNegotiator, TOKEN_USERNAME, TOKEN_MAX_LENGTH, clientInfo);
    assertThat(negotiator.attemptTokenAuthentication(createClientResponse(TOKEN_USERNAME, TOKEN)))
        .isFalse();
    assertThat(negotiator.isComplete()).isFalse();
  }

  @Test
  public void authServiceThrowsUnauthorized() throws UnauthorizedException, IOException {
    AuthenticationService authentication = mock(AuthenticationService.class);
    when(authentication.validateToken(TOKEN, clientInfo))
        .thenThrow(new UnauthorizedException("Not authorized"));

    SaslNegotiator wrappedNegotiator = mock(SaslNegotiator.class);
    when(wrappedNegotiator.isComplete()).thenReturn(false);

    PlainTextJwtTokenSaslNegotiator negotiator =
        new PlainTextJwtTokenSaslNegotiator(
            authentication, wrappedNegotiator, TOKEN_USERNAME, TOKEN_MAX_LENGTH, clientInfo);
    assertThat(negotiator.attemptTokenAuthentication(createClientResponse(TOKEN_USERNAME, TOKEN)))
        .isFalse();
    assertThat(negotiator.isComplete()).isFalse();
  }

  private static byte[] createClientResponse(String username, String password) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    bytes.write(0);
    bytes.write(username.getBytes(StandardCharsets.UTF_8));
    bytes.write(0);
    bytes.write(password.getBytes(StandardCharsets.UTF_8));
    return bytes.toByteArray();
  }
}
