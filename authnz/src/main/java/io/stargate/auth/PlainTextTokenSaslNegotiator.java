package io.stargate.auth;

import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import io.stargate.db.AuthenticatedUser;
import io.stargate.db.Authenticator;
import io.stargate.db.Authenticator.SaslNegotiator;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.cassandra.stargate.exceptions.AuthenticationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PlainTextTokenSaslNegotiator implements SaslNegotiator {

  private static final Logger logger = LoggerFactory.getLogger(PlainTextTokenSaslNegotiator.class);

  static final byte NUL = 0;

  protected final AuthenticationService authentication;
  private final Authenticator.SaslNegotiator wrapped;
  protected StoredCredentials storedCredentials;
  protected final String tokenUsername;
  protected final int tokenMaxLength;

  public PlainTextTokenSaslNegotiator(
      AuthenticationService authentication,
      SaslNegotiator wrapped,
      String tokenUsername,
      int tokenMaxLength) {
    this.authentication = authentication;
    this.wrapped = wrapped;
    this.tokenUsername = tokenUsername;
    this.tokenMaxLength = tokenMaxLength;
  }

  @Override
  public byte[] evaluateResponse(byte[] clientResponse) throws AuthenticationException {
    if (attemptTokenAuthentication(clientResponse)) {
      return null;
    }
    return wrapped.evaluateResponse(clientResponse);
  }

  @Override
  public boolean isComplete() {
    return storedCredentials != null || wrapped.isComplete();
  }

  @Override
  public AuthenticatedUser getAuthenticatedUser() throws AuthenticationException {
    if (storedCredentials != null) {
      return AuthenticatedUser.of(storedCredentials.getRoleName(), storedCredentials.getPassword());
    } else {
      return wrapped.getAuthenticatedUser();
    }
  }

  public abstract boolean attemptTokenAuthentication(byte[] clientResponse);

  /**
   * Copy of the private method:
   * org.apache.cassandra.auth.PasswordAuthenticator.PlainTextSaslAuthenticator#decodeCredentials(byte[]).
   *
   * @param bytes encoded credentials string sent by the client
   * @return a pair contain the username and password
   * @throws AuthenticationException if either the authnId or password is null
   */
  @VisibleForTesting
  public static Credentials decodeCredentials(byte[] bytes) throws AuthenticationException {
    logger.trace("Decoding credentials from client token");
    byte[] user = null;
    byte[] pass = null;
    int end = bytes.length;
    for (int i = bytes.length - 1; i >= 0; i--) {
      if (bytes[i] == NUL) {
        if (pass == null) {
          pass = Arrays.copyOfRange(bytes, i + 1, end);
        } else if (user == null) {
          user = Arrays.copyOfRange(bytes, i + 1, end);
        } else {
          throw new AuthenticationException(
              "Credential format error: username or password is empty or contains NUL(\\0) character");
        }

        end = i;
      }
    }

    if (pass == null || pass.length == 0) {
      throw new AuthenticationException("Password must not be null");
    }
    if (user == null || user.length == 0) {
      throw new AuthenticationException("Authentication ID must not be null");
    }

    return new Credentials(
        new String(user, StandardCharsets.UTF_8), new String(pass, StandardCharsets.UTF_8));
  }
}
