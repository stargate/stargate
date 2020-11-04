package io.stargate.auth.jwt;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.jwk.source.RemoteJWKSet;
import com.nimbusds.jose.proc.JWSKeySelector;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.proc.ConfigurableJWTProcessor;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import io.stargate.auth.AuthenticationService;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Hashtable;
import net.jcip.annotations.GuardedBy;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthJWTServiceActivator implements BundleActivator {

  private static final Logger log = LoggerFactory.getLogger(AuthJWTServiceActivator.class);

  public static final String AUTH_JWT_IDENTIFIER = "AuthnJwtService";

  private static final Hashtable<String, String> props = new Hashtable<>();

  static {
    props.put("AuthIdentifier", AUTH_JWT_IDENTIFIER);
  }

  @GuardedBy("this")
  private AuthnJwtService authnJwtService;

  @Override
  public synchronized void start(BundleContext context) {
    if (authnJwtService == null
        && AUTH_JWT_IDENTIFIER.equals(System.getProperty("stargate.auth_id"))) {
      log.info("Registering authnJwtService in AuthnJwtService");

      String urlProvider = System.getProperty("stargate.auth.jwt_provider_url");
      if (urlProvider == null || urlProvider.equals("")) {
        throw new RuntimeException("Property `stargate.auth.jwt_provider_url` must be set");
      }

      ConfigurableJWTProcessor<SecurityContext> jwtProcessor = new DefaultJWTProcessor<>();
      // Pull the public RSA keys from the provided well-known URL to validate the JWT signature.
      JWKSource<SecurityContext> keySource;
      try {
        // by default this will cache the JWK for 15 minutes
        keySource = new RemoteJWKSet<>(new URL(urlProvider));
      } catch (MalformedURLException e) {
        log.error("Failed to create JwtValidator", e);
        throw new RuntimeException("Failed to create JwtValidator: " + e.getMessage(), e);
      }

      // The expected JWS algorithm of the access tokens
      JWSAlgorithm expectedJWSAlg = JWSAlgorithm.RS256;

      JWSKeySelector<SecurityContext> keySelector =
          new JWSVerificationKeySelector<>(expectedJWSAlg, keySource);
      jwtProcessor.setJWSKeySelector(keySelector);

      authnJwtService = new AuthnJwtService(jwtProcessor);
      context.registerService(AuthenticationService.class.getName(), authnJwtService, props);
    }
  }

  @Override
  public void stop(BundleContext context) {}
}
