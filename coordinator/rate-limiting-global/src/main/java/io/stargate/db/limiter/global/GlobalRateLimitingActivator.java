package io.stargate.db.limiter.global;

import io.stargate.core.activator.BaseActivator;
import io.stargate.db.DbActivator;
import io.stargate.db.limiter.RateLimitingManager;
import io.stargate.db.limiter.global.impl.GlobalRateLimitingManager;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;

/**
 * Activator for the {@link GlobalRateLimitingManager} rate limiting service.
 *
 * <p>For this service to activate, the {@link #IDENTIFIER} value needs to be passed to the {@link
 * DbActivator#RATE_LIMITING_ID_PROPERTY)} system property (see {@link DbActivator}).
 */
public class GlobalRateLimitingActivator extends BaseActivator {
  public static final String IDENTIFIER = "GlobalRateLimiting";
  private static final boolean IS_ENABLED =
      IDENTIFIER.equalsIgnoreCase(System.getProperty(DbActivator.RATE_LIMITING_ID_PROPERTY));

  public GlobalRateLimitingActivator() {
    super("Global Rate Limiting");
  }

  @Override
  protected ServiceAndProperties createService() {
    // If rate limiting is not enabled (or at least not configured to use this rate limiting
    // service), we avoid creating the manager, as the manager would throw if it doesn't find
    // its configuration. Maybe that's a bit ugly and we should instead rely on user not using
    // this service not including the bundle on the classpath at all instead?
    if (!IS_ENABLED) {
      return null;
    }
    GlobalRateLimitingManager manager = new GlobalRateLimitingManager();
    return new ServiceAndProperties(manager, RateLimitingManager.class, properties());
  }

  private static Hashtable<String, String> properties() {
    Hashtable<String, String> props = new Hashtable<>();
    props.put("Identifier", IDENTIFIER);
    return props;
  }

  @Override
  protected List<ServicePointer<?>> dependencies() {
    return Collections.emptyList();
  }
}
