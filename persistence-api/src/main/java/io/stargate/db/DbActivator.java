package io.stargate.db;

import io.stargate.core.activator.BaseActivator;
import io.stargate.db.datastore.DataStoreFactory;
import java.util.Collections;
import java.util.List;

public class DbActivator extends BaseActivator {
  private final ServicePointer<Persistence> persistence =
      BaseActivator.ServicePointer.create(Persistence.class);

  public DbActivator() {
    super("DB services");
  }

  @Override
  protected ServiceAndProperties createService() {
    return new ServiceAndProperties(new DataStoreFactory(), DataStoreFactory.class);
  }

  @Override
  protected void stopService() {
    // no-op
  }

  @Override
  protected List<ServicePointer<?>> dependencies() {
    return Collections.singletonList(persistence);
  }
}
