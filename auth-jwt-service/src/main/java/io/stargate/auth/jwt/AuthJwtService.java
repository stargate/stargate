package io.stargate.auth.jwt;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.proc.BadJOSEException;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.proc.ConfigurableJWTProcessor;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.StoredCredentials;
import io.stargate.auth.UnauthorizedException;
import java.text.ParseException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthJwtService implements AuthenticationService {

  private static final Logger logger = LoggerFactory.getLogger(AuthJwtService.class);

  //  private final JwtValidator jwtValidator;
  private static final String ROLE_FIELD = "x-stargate-role";
  private static final String CLAIMS_FIELD = "stargate_claims";

  private final ConfigurableJWTProcessor<? extends SecurityContext> jwtProcessor;

  public AuthJwtService(ConfigurableJWTProcessor<? extends SecurityContext> jwtProcessor) {
    this.jwtProcessor = jwtProcessor;
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
    JWTClaimsSet claimsSet = validate(token);
    String roleName;
    try {
      roleName = getRoleForJWT(claimsSet.getJSONObjectClaim(CLAIMS_FIELD));
    } catch (IllegalArgumentException | ParseException e) {
      logger.info("Failed to parse claim from JWT", e);
      throw new UnauthorizedException("Failed to parse claim from JWT", e);
    }

    if (roleName == null || roleName.equals("")) {
      throw new UnauthorizedException("JWT must have a value for " + ROLE_FIELD);
    }

    StoredCredentials storedCredentials = new StoredCredentials();
    storedCredentials.setRoleName(roleName);
    return storedCredentials;
  }

  /**
   * For a given JWT check that it is valid which means
   *
   * <p>
   *
   * <ol>
   *   <li>Hasn't expired
   *   <li>Properly signed
   *   <li>Isn't malformed
   * </ol>
   *
   * <p>
   *
   * @param token The JWT to be validated
   * @return Will return the {@link JWTClaimsSet} if the token is valid, otherwise an exception will
   *     be thrown.
   * @throws UnauthorizedException The exception returned for JWTs that are known invalid such as
   *     expired or not signed. If an error occurs while parsing a RuntimeException is thrown.
   */
  private JWTClaimsSet validate(String token) throws UnauthorizedException {
    JWTClaimsSet claimsSet;
    try {
      claimsSet = jwtProcessor.process(token, null); // context is an optional param so passing null
    } catch (ParseException | JOSEException e) {
      logger.info("Failed to process JWT", e);
      throw new UnauthorizedException("Failed to process JWT: " + e.getMessage(), e);
    } catch (BadJOSEException badJOSEException) {
      logger.info("Tried to validate invalid JWT", badJOSEException);
      throw new UnauthorizedException(
          "Invalid JWT: " + badJOSEException.getMessage(), badJOSEException);
    }

    return claimsSet;
  }

  private String getRoleForJWT(Map<String, Object> stargate_claims) {
    if (stargate_claims == null) {
      throw new IllegalArgumentException("Missing field " + ROLE_FIELD + " for JWT");
    }

    if (!(stargate_claims.get(ROLE_FIELD) instanceof String)) {
      throw new IllegalArgumentException("Field " + ROLE_FIELD + " must be of type String");
    }

    return (String) stargate_claims.get(ROLE_FIELD);
  }
}
