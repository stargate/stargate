package io.stargate.auth.jwt;

import com.nimbusds.jwt.JWTClaimsSet;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.StoredCredentials;
import io.stargate.auth.UnauthorizedException;
import java.text.ParseException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthJwtService implements AuthenticationService {

  private static final Logger logger = LoggerFactory.getLogger(AuthJwtService.class);

  private final JwtValidator jwtValidator;
  private static final String ROLE_FIELD = "x-stargate-role";
  private static final String CLAIMS_FIELD = "stargate_claims";

  public AuthJwtService(JwtValidator jwtValidator) {
    this.jwtValidator = jwtValidator;
  }

  @Override
  public String createToken(String key, String secret) {
    throw new UnsupportedOperationException(
        "Creating a token is not supported for AuthJwtService. Tokens must be created out of band.");
  }

  @Override
  public String createToken(String key) {
    throw new UnsupportedOperationException(
        "Creating a token is not supported for AuthJwtService. Tokens must be created out of band.");
  }

  /**
   * Validates a token in the form of a JWT to ensure that 1) it's not expired, 2) it's correctly
   * signed by the provider, and 3) contains the proper role for the given DB.
   *
   * @param token A JWT created by an auth provider.
   * @return A {@link StoredCredentials} containing the role name the request is authenticated to
   *     use.
   * @throws UnauthorizedException An UnauthorizedException if the JWT is expired, malformed, or not
   *     properly signed.
   */
  @Override
  public StoredCredentials validateToken(String token) throws UnauthorizedException {
    JWTClaimsSet claimsSet = jwtValidator.validate(token);
    String roleName;
    try {
      roleName = getRoleForJWT(claimsSet.getJSONObjectClaim(CLAIMS_FIELD));
    } catch (ParseException e) {
      logger.error("Failed to parse claim from JWT", e);
      throw new RuntimeException(e);
    }

    if (roleName == null || roleName.equals("")) {
      throw new UnauthorizedException("JWT must have a value for " + ROLE_FIELD);
    }

    StoredCredentials storedCredentials = new StoredCredentials();
    storedCredentials.setRoleName(roleName);
    return storedCredentials;
  }

  private String getRoleForJWT(Map<String, Object> stargate_claims) throws ParseException {
    if (stargate_claims == null) {
      throw new ParseException("Missing field " + ROLE_FIELD + " for JWT", 0);
    }
    return (String) stargate_claims.get(ROLE_FIELD);
  }
}
