package io.stargate.auth.jwt;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.jwk.source.RemoteJWKSet;
import com.nimbusds.jose.proc.BadJOSEException;
import com.nimbusds.jose.proc.JWSKeySelector;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.proc.ConfigurableJWTProcessor;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import io.stargate.auth.UnauthorizedException;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JwtValidator {

  private static final Logger logger = LoggerFactory.getLogger(JwtValidator.class);

  // Set up a JWT processor to parse the tokens and then check their signature
  // and validity time window (bounded by the "iat", "nbf" and "exp" claims)
  private final ConfigurableJWTProcessor<SecurityContext> jwtProcessor;

  public JwtValidator(String providerURL) throws MalformedURLException {
    this.jwtProcessor = new DefaultJWTProcessor<>();

    // Pull the public RSA keys from the provided well-known URL to validate the JWT signature.
    JWKSource<SecurityContext> keySource =
        new RemoteJWKSet<>(
            new URL(providerURL)); // by default this will cache the JWK for 15 minutes

    // The expected JWS algorithm of the access tokens
    JWSAlgorithm expectedJWSAlg = JWSAlgorithm.RS256;

    JWSKeySelector<SecurityContext> keySelector =
        new JWSVerificationKeySelector<>(expectedJWSAlg, keySource);
    jwtProcessor.setJWSKeySelector(keySelector);
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
  public JWTClaimsSet validate(String token) throws UnauthorizedException {
    JWTClaimsSet claimsSet;
    try {
      claimsSet = jwtProcessor.process(token, null); // context is an optional param so passing null
    } catch (ParseException | JOSEException e) {
      logger.error("Failed to process JWT", e);
      throw new RuntimeException(e);
    } catch (BadJOSEException badJOSEException) {
      logger.info("Tried to validate invalid JWT", badJOSEException);
      throw new UnauthorizedException("Invalid JWT");
    }

    return claimsSet;
  }
}
