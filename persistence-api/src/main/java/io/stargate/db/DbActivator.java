package io.stargate.db;

import io.stargate.core.activator.BaseActivator;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.datastore.PersistenceDataStoreFactory;
import java.util.Collections;
import java.util.List;

public class DbActivator extends BaseActivator {
  private final ServicePointer<Persistence> persistence =
      BaseActivator.ServicePointer.create(
          Persistence.class,
          "Identifier",
          System.getProperty("stargate.persistence_id", "CassandraPersistence"));

  public DbActivator() {
    super("DB services");
  }

  @Override
  protected ServiceAndProperties createService() {
    return new ServiceAndProperties(
        new PersistenceDataStoreFactory(persistence.get()), DataStoreFactory.class);
  }

  @Override
  protected List<ServicePointer<?>> dependencies() {
    return Collections.singletonList(persistence);
  }
}
