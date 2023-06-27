package io.stargate.db.configuration;

import io.stargate.db.Persistence;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.datastore.PersistenceDataStoreFactory;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

/** Responsible for creating a data-store factory. */
public class DataStoreConfiguration {

  /**
   * Creates data store factory based on the Persistence.
   *
   * <p>NOTE: In OSGi this was in io.stargate.db.DbActivator class.
   */
  @ApplicationScoped
  @Produces
  public DataStoreFactory dataStoreFactory(Persistence persistence) {
    return new PersistenceDataStoreFactory(persistence);
  }
}
