package io.stargate.health;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.ClassLoadingGaugeSet;
import com.codahale.metrics.jvm.FileDescriptorRatioGauge;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.JvmAttributeGaugeSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.dropwizard.Application;
import io.dropwizard.cli.Cli;
import io.dropwizard.configuration.ResourceConfigurationSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.util.JarLocation;
import io.stargate.core.metrics.api.HttpMetricsTagProvider;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.core.metrics.api.MetricsScraper;
import io.stargate.metrics.jersey.MetricsBinder;
import java.lang.management.ManagementFactory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ServerProperties;

public class Server extends Application<ApplicationConfiguration> {

  private final BundleService bundleService;
  private final Metrics metrics;
  private final MetricsScraper metricsScraper;
  private final HttpMetricsTagProvider httpMetricsTagProvider;
  private final HealthCheckRegistry healthCheckRegistry;

  public Server(
      BundleService bundleService,
      Metrics metrics,
      MetricsScraper metricsScraper,
      HttpMetricsTagProvider httpMetricsTagProvider,
      HealthCheckRegistry healthCheckRegistry) {
    this.bundleService = bundleService;
    this.metrics = metrics;
    this.metricsScraper = metricsScraper;
    this.httpMetricsTagProvider = httpMetricsTagProvider;
    this.healthCheckRegistry = healthCheckRegistry;
  }

  /**
   * The only reason we override this is to remove the call to {@code bootstrap.registerMetrics()}.
   */
  @Override
  public void run(String... arguments) {
    final Bootstrap<ApplicationConfiguration> bootstrap = new Bootstrap<>(this);
    addDefaultCommands(bootstrap);
    initialize(bootstrap);

    registerJvmMetrics();

    final Cli cli = new Cli(new JarLocation(getClass()), bootstrap, System.out, System.err);
    // only exit if there's an error running the command
    cli.run(arguments).ifPresent(this::onFatalError);
  }

  @Override
  public void run(
      final ApplicationConfiguration applicationConfiguration, final Environment environment) {

    environment.getObjectMapper().configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    environment.getObjectMapper().registerModule(new JavaTimeModule());

    environment
        .jersey()
        .register(
            new AbstractBinder() {
              @Override
              protected void configure() {
                bind(bundleService).to(BundleService.class);
                bind(healthCheckRegistry).to(HealthCheckRegistry.class);
                bind(metricsScraper).to(MetricsScraper.class);
              }
            });
    environment.jersey().register(CheckerResource.class);
    environment.jersey().register(PrometheusResource.class);

    MetricsBinder metricsBinder =
        new MetricsBinder(metrics, httpMetricsTagProvider, HealthCheckerActivator.MODULE_NAME);
    metricsBinder.register(environment.jersey());

    // no html content
    environment.jersey().property(ServerProperties.RESPONSE_SET_STATUS_OVER_SEND_ERROR, true);
  }

  @Override
  public void initialize(final Bootstrap<ApplicationConfiguration> bootstrap) {
    super.initialize(bootstrap);
    bootstrap.setConfigurationSourceProvider(new ResourceConfigurationSourceProvider());
    bootstrap.setMetricRegistry(metrics.getRegistry(HealthCheckerActivator.MODULE_NAME));
    bootstrap.setHealthCheckRegistry(healthCheckRegistry);
  }

  private void registerJvmMetrics() {
    // Register JVM metrics at the top level (no 'health-checker' prefix).
    // This is done only once here, other modules disable them because they would be duplicates.
    MetricRegistry topLevelRegistry = metrics.getRegistry();
    topLevelRegistry.register("jvm.attribute", new JvmAttributeGaugeSet());
    topLevelRegistry.register(
        "jvm.buffers", new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer()));
    topLevelRegistry.register("jvm.classloader", new ClassLoadingGaugeSet());
    topLevelRegistry.register("jvm.filedescriptor", new FileDescriptorRatioGauge());
    topLevelRegistry.register("jvm.gc", new GarbageCollectorMetricSet());
    topLevelRegistry.register("jvm.memory", new MemoryUsageGaugeSet());
    topLevelRegistry.register("jvm.threads", new ThreadStatesGaugeSet());
    topLevelRegistry.register("cpu", new CPUGaugeMetricSet());
  }

  @Override
  protected void bootstrapLogging() {
    // disable dropwizard logging, it will use external logback
  }
}
