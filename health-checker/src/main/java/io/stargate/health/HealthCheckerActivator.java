package io.stargate.health;

import com.codahale.metrics.health.HealthCheckRegistry;
import io.stargate.core.activator.BaseActivator;
import io.stargate.core.metrics.api.HttpMetricsTagProvider;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.core.metrics.api.MetricsScraper;
import io.stargate.db.datastore.DataStoreFactory;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HealthCheckerActivator extends BaseActivator {

  private static final Logger log = LoggerFactory.getLogger(HealthCheckerActivator.class);

  public static final String MODULE_NAME = "health-checker";
  public static final String BUNDLES_CHECK_NAME = "bundles";
  public static final String STORAGE_CHECK_NAME = "storage";
  public static final String DATA_STORE_CHECK_NAME = "datastore";
  public static final String SCHEMA_CHECK_NAME = "schema-agreement";

  private ServicePointer<Metrics> metrics = ServicePointer.create(Metrics.class);
  private ServicePointer<MetricsScraper> metricsScraper =
      ServicePointer.create(MetricsScraper.class);
  private ServicePointer<HttpMetricsTagProvider> httpTagProvider =
      ServicePointer.create(HttpMetricsTagProvider.class);
  private ServicePointer<DataStoreFactory> dataStoreFactory =
      ServicePointer.create(DataStoreFactory.class);
  private ServicePointer<HealthCheckRegistry> healthCheckRegistry =
      ServicePointer.create(HealthCheckRegistry.class);

  public HealthCheckerActivator() {
    super("healthchecker");
  }

  @Override
  public synchronized void stop(BundleContext context) {
    healthCheckRegistry.get().unregister(BUNDLES_CHECK_NAME);
    healthCheckRegistry.get().unregister(DATA_STORE_CHECK_NAME);
    healthCheckRegistry.get().unregister(STORAGE_CHECK_NAME);
  }

  @Nullable
  @Override
  protected ServiceAndProperties createService() {
    log.info("Starting healthchecker....");
    try {
      healthCheckRegistry.get().register(BUNDLES_CHECK_NAME, new BundleStateChecker(context));
      healthCheckRegistry
          .get()
          .register(DATA_STORE_CHECK_NAME, new DataStoreHealthChecker(dataStoreFactory.get()));
      healthCheckRegistry
          .get()
          .register(STORAGE_CHECK_NAME, new StorageHealthChecker(dataStoreFactory.get()));
      healthCheckRegistry.get().register(SCHEMA_CHECK_NAME, new SchemaAgreementChecker(context));

      WebImpl web =
          new WebImpl(
              context,
              metrics.get(),
              metricsScraper.get(),
              httpTagProvider.get(),
              healthCheckRegistry.get());
      web.start();
      log.info("Started healthchecker....");
    } catch (Exception e) {
      log.error("Failed", e);
    }
    return null;
  }

  @Override
  protected List<ServicePointer<?>> dependencies() {
    return Arrays.asList(
        metrics, metricsScraper, httpTagProvider, healthCheckRegistry, dataStoreFactory);
  }
}
