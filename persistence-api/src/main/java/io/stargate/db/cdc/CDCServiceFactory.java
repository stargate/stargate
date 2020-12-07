package io.stargate.db.cdc;

import io.stargate.config.store.api.ConfigStore;
import io.stargate.db.cdc.datastore.CDCQueryBuilder;
import io.stargate.db.datastore.DataStore;

public class CDCServiceFactory {
  private final CDCQueryBuilder cdcQueryBuilder;
  private final DataStore dataStore;

  public CDCServiceFactory(ConfigStore configStore, DataStore dataStore) {
    this.dataStore = dataStore;
    this.cdcQueryBuilder = new CDCQueryBuilder(configStore);
  }

  public void create() {
    cdcQueryBuilder.initCDCKeyspaceAndTable(dataStore);
  }
}
