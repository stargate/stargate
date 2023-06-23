package io.stargate.db;

import io.stargate.core.activator.BaseActivator;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.datastore.PersistenceDataStoreFactory;
import io.stargate.db.metrics.api.ClientInfoMetricsTagProvider;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

/** Activator for the {@link DataStoreFactory} service. */
public class DbActivator extends BaseActivator {

  public static final String PERSISTENCE_IDENTIFIER = "StargatePersistence";

  private static final String DB_PERSISTENCE_IDENTIFIER =
      System.getProperty("stargate.persistence_id", "CassandraPersistence");

  private static final String CLIENT_INFO_TAG_PROVIDER_ID =
      System.getProperty("stargate.metrics.client_info_tag_provider.id");

  private final ServicePointer<Persistence> dbPersistence =
      BaseActivator.ServicePointer.create(
          Persistence.class, "Identifier", DB_PERSISTENCE_IDENTIFIER);

  public DbActivator() {
    super("DB services");
  }

  @Override
  protected List<ServiceAndProperties> createServices() {
    Persistence persistence = this.dbPersistence.get();

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
    return deps;
  }
}
