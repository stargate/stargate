package io.stargate.db;

import io.stargate.core.activator.BaseActivator;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.datastore.PersistenceDataStoreFactory;
import io.stargate.db.limiter.RateLimitingManager;
import io.stargate.db.metrics.api.ClientInfoMetricsTagProvider;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

/**
 * Activator for the {@link DataStoreFactory} service and, if enabled, the {@link
 * RateLimitingPersistence} one.
 *
 * <p>For rate limiting to be activated, a service implementing {@link RateLimitingManager} first
 * needs to be activated/registered with an "Identifier" property set to some value X, and the
 * {@link #RATE_LIMITING_ID_PROPERTY} system property needs to be set to that X value. This is done
 * to avoid having rate limiting activated by mistake, just because a bundle that activates a {@link
 * RateLimitingManager} is present on the classpath (meaning, setting the {@link
 * #RATE_LIMITING_ID_PROPERTY} acts as a confirmation that this rate limiting needs to indeed be
 * activated).
 */
public class DbActivator extends BaseActivator {

  public static final String PERSISTENCE_IDENTIFIER = "StargatePersistence";

  public static final String RATE_LIMITING_ID_PROPERTY = "stargate.limiter.id";

  private static final String DB_PERSISTENCE_IDENTIFIER =
      System.getProperty("stargate.persistence_id", "CassandraPersistence");

  private static final String RATE_LIMITING_IDENTIFIER =
      System.getProperty(RATE_LIMITING_ID_PROPERTY, "<none>");

  private static final String CLIENT_INFO_TAG_PROVIDER_ID =
      System.getProperty("stargate.metrics.client_info_tag_provider.id");

  private final ServicePointer<Persistence> dbPersistence =
      BaseActivator.ServicePointer.create(
          Persistence.class, "Identifier", DB_PERSISTENCE_IDENTIFIER);

  private final ServicePointer<RateLimitingManager> rateLimitingManager =
      ServicePointer.create(RateLimitingManager.class, "Identifier", RATE_LIMITING_IDENTIFIER);

  public DbActivator() {
    super("DB services");
  }

  private boolean hasRateLimitingEnabled() {
    return !RATE_LIMITING_IDENTIFIER.equalsIgnoreCase("<none>");
  }

  @Override
  protected List<ServiceAndProperties> createServices() {
    Persistence persistence = this.dbPersistence.get();
    if (hasRateLimitingEnabled()) {
      RateLimitingManager rateLimiter = rateLimitingManager.get();
      if (rateLimiter == null) {
        throw new RuntimeException(
            String.format(
                "Could not find rate limiter service with id '%s'", RATE_LIMITING_IDENTIFIER));
      }
      persistence = new RateLimitingPersistence(persistence, rateLimiter);
    }

    List<ServiceAndProperties> services = new ArrayList<>();
    services.add(
        new ServiceAndProperties(persistence, Persistence.class, stargatePersistenceProperties()));
    services.add(
        new ServiceAndProperties(
            new PersistenceDataStoreFactory(persistence), DataStoreFactory.class));

    // if no specific client info tag provider, add default
    if (null == CLIENT_INFO_TAG_PROVIDER_ID) {
      services.add(
          new ServiceAndProperties(
              ClientInfoMetricsTagProvider.DEFAULT, ClientInfoMetricsTagProvider.class));
    }

    return services;
  }

  private static Hashtable<String, String> stargatePersistenceProperties() {
    Hashtable<String, String> props = new Hashtable<>();
    props.put("Identifier", PERSISTENCE_IDENTIFIER);
    return props;
  }

  @Override
  protected List<ServicePointer<?>> dependencies() {
    List<ServicePointer<?>> deps = new ArrayList<>(2);
    deps.add(dbPersistence);
    if (hasRateLimitingEnabled()) {
      deps.add(rateLimitingManager);
    }
    return deps;
  }
}
