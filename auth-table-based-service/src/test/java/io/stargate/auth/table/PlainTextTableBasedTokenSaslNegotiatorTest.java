package io.stargate.auth.table;

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

class PlainTextTableBasedTokenSaslNegotiatorTest {

  private final String TOKEN = "a24b121a-a385-44a6-8ae1-fe7542dbc490";
  private final String ROLE = "someRole";
  private final int TOKEN_MAX_LENGTH = 36;
  private final String TOKEN_USERNAME = "token";
  private final ClientInfo clientInfo =
      new ClientInfo(
          InetSocketAddress.createUnresolved("localhost", 1234),
          InetSocketAddress.createUnresolved("localhost", 4321));

  @Test
  public void decodeCredentials() {
    Credentials credentials =
        PlainTextTableBasedTokenSaslNegotiator.decodeCredentials(
            new byte[] {0, 97, 98, 99, 0, 97, 98, 99});
    assertThat(credentials.getUsername()).isEqualTo("abc");
    assertThat(credentials.getPassword()).isEqualTo(new char[] {'a', 'b', 'c'});
  }

  @Test
  public void invalidDecodeCredentials() {
    // Empty
    assertThatThrownBy(
            () -> PlainTextTableBasedTokenSaslNegotiator.decodeCredentials(new byte[] {}))
        .isInstanceOf(AuthenticationException.class)
        .hasMessage("Password must not be null");

    // Empty authzid
    assertThatThrownBy(
            () -> PlainTextTableBasedTokenSaslNegotiator.decodeCredentials(new byte[] {0}))
        .isInstanceOf(AuthenticationException.class)
        .hasMessage("Password must not be null");

    // Empty authzid and authnid
    assertThatThrownBy(
            () -> PlainTextTableBasedTokenSaslNegotiator.decodeCredentials(new byte[] {0, 0}))
        .isInstanceOf(AuthenticationException.class)
        .hasMessage("Password must not be null");

    // Only authzid
    assertThatThrownBy(
            () ->
                PlainTextTableBasedTokenSaslNegotiator.decodeCredentials(
                    new byte[] {97, 98, 99, 0, 0}))
        .isInstanceOf(AuthenticationException.class)
        .hasMessage("Password must not be null");

    // Empty authzid, but valid authnid and password
    assertThatThrownBy(
            () ->
                PlainTextTableBasedTokenSaslNegotiator.decodeCredentials(
                    new byte[] {0, 97, 0, 97, 0}))
        .isInstanceOf(AuthenticationException.class)
        .hasMessage(
            "Credential format error: username or password is empty or contains NUL(\\0) character");

    // Non-empty authzid
    assertThatThrownBy(
            () ->
                PlainTextTableBasedTokenSaslNegotiator.decodeCredentials(
                    new byte[] {97, 0, 97, 0, 97, 0}))
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
        new PlainTextTableBasedTokenSaslNegotiator(
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
        new PlainTextTableBasedTokenSaslNegotiator(
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
        new PlainTextTableBasedTokenSaslNegotiator(
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
        new PlainTextTableBasedTokenSaslNegotiator(
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

    PlainTextTokenSaslNegotiator negotiator =
        new PlainTextTableBasedTokenSaslNegotiator(
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
