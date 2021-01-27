package io.stargate.health;

import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BundleService {
  private static final Logger logger = LoggerFactory.getLogger(BundleService.class);

  private static final String HEALTH_NAME_HEADER = "x-Stargate-Health-Checkers";

  private final BundleContext context;

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

  public boolean checkDataStoreAvailable() {
    try {
      ServiceReference<DataStoreFactory> dataStoreFactoryReference =
          context.getServiceReference(DataStoreFactory.class);
      if (dataStoreFactoryReference != null) {
        DataStoreFactory dataStoreFactory = context.getService(dataStoreFactoryReference);
        try {
          DataStore dataStore = dataStoreFactory.createInternal();

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
        } finally {
          context.ungetService(dataStoreFactoryReference);
        }
      }

      logger.warn("DataStoreFactory service is not available");
    } catch (Exception e) {
      logger.warn("checkIsReady failed with", e);
      return false;
    }

    return false;
  }

  public Set<String> defaultHeathCheckNames() {
    Set<String> result = new HashSet<>();
    for (Bundle bundle : context.getBundles()) {
      String header = bundle.getHeaders().get(HEALTH_NAME_HEADER);
      if (header != null) {
        result.addAll(Arrays.asList(header.split(",")));
      }
    }

    return result;
  }
}
