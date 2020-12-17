package io.stargate.health;

import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import java.util.UUID;
import java.util.concurrent.Future;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BundleService {
  private static final Logger logger = LoggerFactory.getLogger(BundleService.class);

  private BundleContext context;

  public BundleService(BundleContext context) {
    this.context = context;
  }

  public boolean checkBundleStates() {
    for (Bundle bundle : context.getBundles()) {
      if (bundle.getState() != Bundle.ACTIVE) {
        logger.warn("bundle: " + bundle.getSymbolicName() + " is not active");
        return false;
      }
    }

    return true;
  }

  public boolean checkIsReady() {
    ServiceReference dataStoreFactoryReference =
        context.getServiceReference(DataStoreFactory.class.getName());
    if (dataStoreFactoryReference != null) {

      try {
        DataStoreFactory dataStoreFactory =
            (DataStoreFactory) context.getService(dataStoreFactoryReference);
        DataStore dataStore = dataStoreFactory.create();

        Future<ResultSet> rs =
            dataStore
                .queryBuilder()
                .select()
                .column("cluster_name")
                .column("schema_version")
                .from("system", "local")
                .build()
                .execute();

        Row row = rs.get().one();
        String clusterName = row.getString("cluster_name");
        UUID schemaVersion = row.getUuid("schema_version");
        return clusterName != null && !"".equals(clusterName) && schemaVersion != null;
      } catch (Exception e) {
        logger.warn("checkIsReady failed with", e);
        return false;
      }
    }

    return false;
  }
}
