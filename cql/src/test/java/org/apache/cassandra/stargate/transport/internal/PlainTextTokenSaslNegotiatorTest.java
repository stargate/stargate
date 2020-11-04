package org.apache.cassandra.stargate.transport.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.stargate.auth.AuthenticationService;
import io.stargate.auth.StoredCredentials;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.AuthenticatedUser;
import io.stargate.db.Authenticator.SaslNegotiator;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.cassandra.stargate.exceptions.AuthenticationException;
import org.apache.cassandra.stargate.transport.internal.PlainTextTokenSaslNegotiator.Credentials;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

public class PlainTextTokenSaslNegotiatorTest {
  private final String TOKEN = "a24b121a-a385-44a6-8ae1-fe7542dbc490";
  private final String ROLE = "someRole";

  @Test
  public void decodeCredentials() {
    Credentials credentials =
        PlainTextTokenSaslNegotiator.decodeCredentials(new byte[] {0, 97, 98, 99, 0, 97, 98, 99});
    assertThat(credentials.username).isEqualTo("abc");
    assertThat(credentials.password).isEqualTo("abc");
  }

  @Test
  public void invalidDecodeCredentials() {
    // Empty
    assertThatThrownBy(() -> PlainTextTokenSaslNegotiator.decodeCredentials(new byte[] {}))
        .isInstanceOf(AuthenticationException.class)
        .hasMessage("Password must not be null");

    // Empty authzid
    assertThatThrownBy(() -> PlainTextTokenSaslNegotiator.decodeCredentials(new byte[] {0}))
        .isInstanceOf(AuthenticationException.class)
        .hasMessage("Password must not be null");

    // Empty authzid and authnid
    assertThatThrownBy(() -> PlainTextTokenSaslNegotiator.decodeCredentials(new byte[] {0, 0}))
        .isInstanceOf(AuthenticationException.class)
        .hasMessage("Password must not be null");

    // Only authzid
    assertThatThrownBy(
            () -> PlainTextTokenSaslNegotiator.decodeCredentials(new byte[] {97, 98, 99, 0, 0}))
        .isInstanceOf(AuthenticationException.class)
        .hasMessage("Password must not be null");

    // Empty authzid, but valid authnid and password
    assertThatThrownBy(
            () -> PlainTextTokenSaslNegotiator.decodeCredentials(new byte[] {0, 97, 0, 97, 0}))
        .isInstanceOf(AuthenticationException.class)
        .hasMessage(
            "Credential format error: username or password is empty or contains NUL(\\0) character");

    // Non-empty authzid
    assertThatThrownBy(
            () -> PlainTextTokenSaslNegotiator.decodeCredentials(new byte[] {97, 0, 97, 0, 97, 0}))
        .isInstanceOf(AuthenticationException.class)
        .hasMessage(
            "Credential format error: username or password is empty or contains NUL(\\0) character");
  }

  @Test
  public void useToken() throws IOException, UnauthorizedException {
    final byte[] clientResponse =
        createClientResponse(PlainTextTokenSaslNegotiator.TOKEN_USERNAME, TOKEN);

    StoredCredentials credentials = new StoredCredentials();
    credentials.setRoleName(ROLE);
    AuthenticationService authentication = mock(AuthenticationService.class);
    when(authentication.validateToken(TOKEN)).thenReturn(credentials);

    PlainTextTokenSaslNegotiator negotiator =
        new PlainTextTokenSaslNegotiator(null, authentication);
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
        new PlainTextTokenSaslNegotiator(wrappedNegotiator, null);
    assertThat(negotiator.evaluateResponse(clientResponse)).isNull();
    assertThat(negotiator.isComplete()).isTrue();
    assertThat(negotiator.getAuthenticatedUser().name()).isEqualTo(ROLE);
  }

  @Test
  public void tokenGreaterThanMaxLength() throws IOException {
    final String tooLongToken =
        StringUtils.repeat("a", PlainTextTokenSaslNegotiator.TOKEN_MAX_LENGTH + 1);

    SaslNegotiator wrappedNegotiator = mock(SaslNegotiator.class);
    when(wrappedNegotiator.isComplete()).thenReturn(false);

    PlainTextTokenSaslNegotiator negotiator =
        new PlainTextTokenSaslNegotiator(wrappedNegotiator, null);
    assertThat(
            negotiator.attemptTokenAuthentication(
                createClientResponse(PlainTextTokenSaslNegotiator.TOKEN_USERNAME, tooLongToken)))
        .isFalse();
    assertThat(negotiator.isComplete()).isFalse();
  }

  @Test
  public void authServiceReturnsNullCredentials() throws UnauthorizedException, IOException {
    AuthenticationService authentication = mock(AuthenticationService.class);
    when(authentication.validateToken(TOKEN)).thenReturn(null);

    SaslNegotiator wrappedNegotiator = mock(SaslNegotiator.class);
    when(wrappedNegotiator.isComplete()).thenReturn(false);

    PlainTextTokenSaslNegotiator negotiator =
        new PlainTextTokenSaslNegotiator(wrappedNegotiator, authentication);
    assertThat(
            negotiator.attemptTokenAuthentication(
                createClientResponse(PlainTextTokenSaslNegotiator.TOKEN_USERNAME, TOKEN)))
        .isFalse();
    assertThat(negotiator.isComplete()).isFalse();
  }

  @Test
  public void authServiceThrowsUnauthorized() throws UnauthorizedException, IOException {
    AuthenticationService authentication = mock(AuthenticationService.class);
    when(authentication.validateToken(TOKEN))
        .thenThrow(new UnauthorizedException("Not authorized"));

    SaslNegotiator wrappedNegotiator = mock(SaslNegotiator.class);
    when(wrappedNegotiator.isComplete()).thenReturn(false);

    PlainTextTokenSaslNegotiator negotiator =
        new PlainTextTokenSaslNegotiator(wrappedNegotiator, authentication);
    assertThat(
            negotiator.attemptTokenAuthentication(
                createClientResponse(PlainTextTokenSaslNegotiator.TOKEN_USERNAME, TOKEN)))
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
