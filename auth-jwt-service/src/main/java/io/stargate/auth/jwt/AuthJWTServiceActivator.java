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

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.jwk.source.RemoteJWKSet;
import com.nimbusds.jose.proc.JWSKeySelector;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.proc.ConfigurableJWTProcessor;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthorizationService;
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

  public static final String AUTH_JWT_IDENTIFIER = "AuthJwtService";

  @SuppressWarnings("JdkObsolete")
  private static final Hashtable<String, String> props = new Hashtable<>();

  static {
    props.put("AuthIdentifier", AUTH_JWT_IDENTIFIER);
  }

  @GuardedBy("this")
  private AuthnJwtService authnJwtService;

  @GuardedBy("this")
  private AuthzJwtService authzJwtService;

  @Override
  public synchronized void start(BundleContext context) {
    if (AUTH_JWT_IDENTIFIER.equals(System.getProperty("stargate.auth_id"))) {
      log.info("Registering authnJwtService and authzJwtService in AuthnJwtService");

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

      authzJwtService = new AuthzJwtService();
      context.registerService(AuthorizationService.class.getName(), authzJwtService, props);
    }
  }

  @Override
  public void stop(BundleContext context) {}
}
