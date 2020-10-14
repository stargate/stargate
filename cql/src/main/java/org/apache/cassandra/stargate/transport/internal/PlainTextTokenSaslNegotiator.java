package org.apache.cassandra.stargate.transport.internal;

import com.google.common.annotations.VisibleForTesting;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.StoredCredentials;
import io.stargate.db.AuthenticatedUser;
import io.stargate.db.Authenticator;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.cassandra.stargate.exceptions.AuthenticationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PlainTextTokenSaslNegotiator implements Authenticator.SaslNegotiator {
  private static final Logger logger = LoggerFactory.getLogger(PlainTextTokenSaslNegotiator.class);

  static final String TOKEN_USERNAME = System.getProperty("stargate.cql_token_username", "token");
  static final int TOKEN_MAX_LENGTH =
      Integer.parseInt(System.getProperty("stargate.cql_token_max_length", "36"));

  static final byte NUL = 0;

  private final AuthenticationService authentication;
  private final Authenticator.SaslNegotiator wrapped;
  private StoredCredentials storedCredentials;

  PlainTextTokenSaslNegotiator(
      Authenticator.SaslNegotiator wrapped, AuthenticationService authentication) {
    this.authentication = authentication;
    this.wrapped = wrapped;
  }

  @Override
  public byte[] evaluateResponse(byte[] clientResponse) throws AuthenticationException {
    if (attemptTokenAuthentication(clientResponse)) return null;
    return wrapped.evaluateResponse(clientResponse);
  }

  @Override
  public boolean isComplete() {
    return storedCredentials != null || wrapped.isComplete();
  }

  @Override
  public AuthenticatedUser getAuthenticatedUser() throws AuthenticationException {
    if (storedCredentials != null) return AuthenticatedUser.of(storedCredentials.getRoleName());
    else return wrapped.getAuthenticatedUser();
  }

  @VisibleForTesting
  boolean attemptTokenAuthentication(byte[] clientResponse) {
    try {
      Credentials credentials = decodeCredentials(clientResponse);

      if (!credentials.username.equals(TOKEN_USERNAME)) return false;

      logger.trace("Attempting to validate token");
      if (credentials.password.length() > TOKEN_MAX_LENGTH) {
        logger.error("Token was too long ({} characters)", credentials.password.length());
        return false;
      }

      storedCredentials = authentication.validateToken(credentials.password);
      if (storedCredentials == null) {
        logger.error("Null credentials returned from authentication service");
        return false;
      }
    } catch (Exception e) {
      logger.error("Unable to validate token", e);
      return false;
    }

    return true;
  }

  /**
   * Copy of the private method:
   * org.apache.cassandra.auth.PasswordAuthenticator.PlainTextSaslAuthenticator#decodeCredentials(byte[]).
   *
   * @param bytes encoded credentials string sent by the client
   * @throws AuthenticationException if either the authnId or password is null
   * @return a pair contain the username and password
   */
  @VisibleForTesting
  static Credentials decodeCredentials(byte[] bytes) throws AuthenticationException {
    logger.trace("Decoding credentials from client token");
    byte[] user = null;
    byte[] pass = null;
    int end = bytes.length;
    for (int i = bytes.length - 1; i >= 0; i--) {
      if (bytes[i] == NUL) {
        if (pass == null) pass = Arrays.copyOfRange(bytes, i + 1, end);
        else if (user == null) user = Arrays.copyOfRange(bytes, i + 1, end);
        else
          throw new AuthenticationException(
              "Credential format error: username or password is empty or contains NUL(\\0) character");

        end = i;
      }
    }

    if (pass == null || pass.length == 0)
      throw new AuthenticationException("Password must not be null");
    if (user == null || user.length == 0)
      throw new AuthenticationException("Authentication ID must not be null");

    return new Credentials(
        new String(user, StandardCharsets.UTF_8), new String(pass, StandardCharsets.UTF_8));
  }

  @VisibleForTesting
  static class Credentials {
    String username;
    String password;

    Credentials(String username, String password) {
      this.username = username;
      this.password = password;
    }
  }
}
