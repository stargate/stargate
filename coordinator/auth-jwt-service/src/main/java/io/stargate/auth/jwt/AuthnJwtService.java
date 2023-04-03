/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.auth.jwt;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.proc.BadJOSEException;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.proc.ConfigurableJWTProcessor;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.Authenticator.SaslNegotiator;
import io.stargate.db.ClientInfo;
import java.text.ParseException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthnJwtService implements AuthenticationService {

  private static final Logger logger = LoggerFactory.getLogger(AuthnJwtService.class);

  protected static final String STARGATE_PREFIX = "x-stargate-";
  protected static final String ROLE_FIELD = STARGATE_PREFIX + "role";
  protected static final String CLAIMS_FIELD = "stargate_claims";

  private final ConfigurableJWTProcessor<? extends SecurityContext> jwtProcessor;

  public AuthnJwtService(ConfigurableJWTProcessor<? extends SecurityContext> jwtProcessor) {
    this.jwtProcessor = jwtProcessor;
  }

  @Override
  public String createToken(String key, String secret, Map<String, String> headers) {
    throw new UnsupportedOperationException(
        "Creating a token is not supported for AuthnJwtService. Tokens must be created out of band.");
  }

  @Override
  public String createToken(String key, Map<String, String> headers) {
    throw new UnsupportedOperationException(
        "Creating a token is not supported for AuthnJwtService. Tokens must be created out of band.");
  }

  /**
   * Validates a token in the form of a JWT to ensure that 1) it's not expired, 2) it's correctly
   * signed by the provider, and 3) contains the proper role for the given DB.
   *
   * @param token A JWT created by an auth provider.
   * @return A {@link AuthenticationSubject} containing the role name the request is authenticated
   *     to use.
   * @throws UnauthorizedException An UnauthorizedException if the JWT is expired, malformed, or not
   *     properly signed.
   */
  @Override
  public AuthenticationSubject validateToken(String token) throws UnauthorizedException {
    if ((token == null) || token.isEmpty()) {
      throw new UnauthorizedException("authorization failed - missing token");
    }

    JWTClaimsSet claimsSet = validate(token);
    String roleName;
    try {
      roleName = getRoleForJWT(claimsSet.getJSONObjectClaim(CLAIMS_FIELD));
    } catch (IllegalArgumentException | ParseException e) {
      logger.info(
          "Failed to parse claim from JWT ({}): {}", e.getClass().getName(), e.getMessage());
      throw new UnauthorizedException("Failed to parse claim from JWT", e);
    }

    if ((roleName == null) || roleName.isEmpty()) {
      throw new UnauthorizedException("JWT must have a value for " + ROLE_FIELD);
    }

    return AuthenticationSubject.of(token, roleName);
  }

  @Override
  public SaslNegotiator getSaslNegotiator(SaslNegotiator wrapped, ClientInfo clientInfo) {
    return new PlainTextJwtTokenSaslNegotiator(
        this,
        wrapped,
        System.getProperty("stargate.cql_token_username", "token"),
        Integer.parseInt(System.getProperty("stargate.cql_token_max_length", "4096")),
        clientInfo);
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
      logger.info("Failed to process JWT ({}): {}", e.getClass().getName(), e.getMessage());
      throw new UnauthorizedException("Failed to process JWT: " + e.getMessage(), e);
    } catch (BadJOSEException badJOSEException) {
      logger.info(
          "Tried to validate invalid JWT (BadJOSEException): {}", badJOSEException.getMessage());
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
