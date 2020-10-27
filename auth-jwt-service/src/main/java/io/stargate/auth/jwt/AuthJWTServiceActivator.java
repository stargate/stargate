package io.stargate.auth.jwt;

import io.stargate.auth.AuthenticationService;
import java.net.MalformedURLException;
import java.util.Hashtable;
import net.jcip.annotations.GuardedBy;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthJWTServiceActivator implements BundleActivator {

  private static final Logger log = LoggerFactory.getLogger(AuthJWTServiceActivator.class);
  private static final Hashtable<String, String> props = new Hashtable<>();

  static {
    props.put("AuthIdentifier", "AuthJwtService");
  }

  @GuardedBy("this")
  private AuthJwtService authJwtService;

  @Override
  public synchronized void start(BundleContext context) {
    if (authJwtService == null) {
      log.info("Registering authJwtService in AuthJwtService");

      JwtValidator jwtValidator;
      try {
        jwtValidator = new JwtValidator(System.getProperty("stargate.auth.jwt_provider_url"));
      } catch (MalformedURLException e) {
        log.error("Failed to create JwtValidator", e);
        throw new RuntimeException(e);
      }

      authJwtService = new AuthJwtService(jwtValidator);
      context.registerService(AuthenticationService.class.getName(), authJwtService, props);
    }
  }

  @Override
  public void stop(BundleContext context) {}
}
