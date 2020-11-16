package io.stargate.auth.jwt;

import io.stargate.auth.AuthenticationService;
import io.stargate.auth.Credentials;
import io.stargate.auth.PlainTextTokenSaslNegotiator;
import io.stargate.db.Authenticator.SaslNegotiator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PlainTextJwtTokenSaslNegotiator extends PlainTextTokenSaslNegotiator {

  private static final Logger logger =
      LoggerFactory.getLogger(PlainTextJwtTokenSaslNegotiator.class);

  public PlainTextJwtTokenSaslNegotiator(
      AuthenticationService authentication,
      SaslNegotiator wrapped,
      String tokenUsername,
      int tokenMaxLength) {
    super(authentication, wrapped, tokenUsername, tokenMaxLength);
  }

  @Override
  public boolean attemptTokenAuthentication(byte[] clientResponse) {
    try {
      Credentials credentials = decodeCredentials(clientResponse);

      if (!credentials.getUsername().equals(tokenUsername)) {
        return false;
      }

      logger.trace("Attempting to validate token");
      // TODO: [doug] 2020-11-12, Thu, 13:45 some other JWT safe check here
      if (credentials.getPassword().length() > tokenMaxLength) {
        logger.error("Token was too long ({} characters)", credentials.getPassword().length());
        return false;
      }

      storedCredentials = authentication.validateToken(credentials.getPassword());
      if (storedCredentials == null) {
        logger.error("Null credentials returned from authentication service");
        return false;
      }
      storedCredentials.setPassword(credentials.getPassword());
    } catch (Exception e) {
      logger.error("Unable to validate token", e);
      return false;
    }

    return true;
  }
}
