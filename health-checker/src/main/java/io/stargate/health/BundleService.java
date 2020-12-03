package io.stargate.health;

import io.stargate.db.Persistence;
import io.stargate.db.datastore.DataStore;
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

  static String PERSISTENCE_IDENTIFIER =
      System.getProperty("stargate.persistence_id", "CassandraPersistence");

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
    ServiceReference persistenceReference =
        context.getServiceReference(Persistence.class.getName());
    if (persistenceReference != null
        && persistenceReference.getProperty("Identifier").equals(PERSISTENCE_IDENTIFIER)) {

      try {
        Persistence persistence = (Persistence) context.getService(persistenceReference);
        DataStore dataStore = DataStore.create(persistence);

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
