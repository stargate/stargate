package io.stargate.health;

import com.codahale.metrics.health.HealthCheckRegistry;
import io.stargate.core.activator.BaseActivator;
import io.stargate.core.metrics.api.Metrics;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.osgi.framework.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HealthCheckerActivator extends BaseActivator {

  private static final Logger log = LoggerFactory.getLogger(HealthCheckerActivator.class);

  public static final String BUNDLES_CHECK_NAME = "bundles";
  public static final String DATASTORE_CHECK_NAME = "datastore";
  public static final String STARGATE_NODES_CHECK_NAME = "stargate-nodes";
  public static final String SCHEMA_CHECK_NAME = "schema-agreement";

  private ServicePointer<Metrics> metrics = ServicePointer.create(Metrics.class);
  private ServicePointer<HealthCheckRegistry> healthCheckRegistry =
      ServicePointer.create(HealthCheckRegistry.class);

  public HealthCheckerActivator() {
    super("healthchecker");
  }

  @Override
  public synchronized void stop(BundleContext context) {
    healthCheckRegistry.get().unregister(BUNDLES_CHECK_NAME);
    healthCheckRegistry.get().unregister(STARGATE_NODES_CHECK_NAME);
    healthCheckRegistry.get().unregister(DATASTORE_CHECK_NAME);
  }

  @Nullable
  @Override
  protected ServiceAndProperties createService() {
    log.info("Starting healthchecker....");
    try {
      healthCheckRegistry.get().register(BUNDLES_CHECK_NAME, new BundleStateChecker(context));
      healthCheckRegistry
          .get()
          .register(STARGATE_NODES_CHECK_NAME, new StargateNodesHealthChecker(context));
      healthCheckRegistry.get().register(DATASTORE_CHECK_NAME, new DataStoreHealthChecker(context));
      healthCheckRegistry.get().register(SCHEMA_CHECK_NAME, new SchemaAgreementChecker(context));

      WebImpl web = new WebImpl(context, metrics.get(), healthCheckRegistry.get());
      web.start();
      log.info("Started healthchecker....");
    } catch (Exception e) {
      log.error("Failed", e);
    }
    return null;
  }

  @Override
  protected List<ServicePointer<?>> dependencies() {
    return Arrays.asList(metrics, healthCheckRegistry);
  }
}
