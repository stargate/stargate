package io.stargate.health;

import com.codahale.metrics.health.HealthCheckRegistry;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.core.metrics.api.MetricsScraper;
import org.osgi.framework.BundleContext;

public class WebImpl {
  private BundleContext context;
  private Metrics metrics;
  private MetricsScraper metricsScraper;
  private final HealthCheckRegistry healthCheckRegistry;

  public WebImpl(
      BundleContext context,
      Metrics metrics,
      MetricsScraper metricsScraper,
      HealthCheckRegistry healthCheckRegistry) {
    this.context = context;
    this.metrics = metrics;
    this.metricsScraper = metricsScraper;
    this.healthCheckRegistry = healthCheckRegistry;
  }

  public void start() throws Exception {
    Server server =
        new Server(new BundleService(context), metrics, metricsScraper, healthCheckRegistry);
    server.run("server", "config.yaml");
  }
}
