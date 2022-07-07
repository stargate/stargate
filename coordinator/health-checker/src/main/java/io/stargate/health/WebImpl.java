package io.stargate.health;

import com.codahale.metrics.health.HealthCheckRegistry;
import io.stargate.core.metrics.api.HttpMetricsTagProvider;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.core.metrics.api.MetricsScraper;
import org.osgi.framework.BundleContext;

public class WebImpl {
  private final BundleContext context;
  private final Metrics metrics;
  private final MetricsScraper metricsScraper;
  private final HttpMetricsTagProvider httpMetricsTagProvider;
  private final HealthCheckRegistry healthCheckRegistry;

  public WebImpl(
      BundleContext context,
      Metrics metrics,
      MetricsScraper metricsScraper,
      HttpMetricsTagProvider httpMetricsTagProvider,
      HealthCheckRegistry healthCheckRegistry) {
    this.context = context;
    this.metrics = metrics;
    this.metricsScraper = metricsScraper;
    this.httpMetricsTagProvider = httpMetricsTagProvider;
    this.healthCheckRegistry = healthCheckRegistry;
  }

  public void start() throws Exception {
    Server server =
        new Server(
            new BundleService(context),
            metrics,
            metricsScraper,
            httpMetricsTagProvider,
            healthCheckRegistry);
    server.run("server", "config.yaml");
  }
}
