package io.stargate.health;

import java.lang.management.ManagementFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.ClassLoadingGaugeSet;
import com.codahale.metrics.jvm.FileDescriptorRatioGauge;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.JvmAttributeGaugeSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import io.dropwizard.cli.Cli;
import io.dropwizard.util.JarLocation;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.stargate.health.metrics.api.Metrics;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.dropwizard.Application;
import io.dropwizard.configuration.ResourceConfigurationSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class Server extends Application<ApplicationConfiguration> {
    private BundleService bundleService;
    private final Metrics metrics;

    public Server(BundleService bundleService, Metrics metrics) {
        this.bundleService = bundleService;
        this.metrics = metrics;
    }

    /** The only reason we override this is to remove the call to {@code bootstrap.registerMetrics()}. */
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
    public void run(final ApplicationConfiguration applicationConfiguration, final Environment environment) {

        environment.getObjectMapper().configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        environment.getObjectMapper().registerModule(new JavaTimeModule());

        environment.jersey().register(new AbstractBinder() {
            @Override
            protected void configure() {
                bind(bundleService).to(BundleService.class);
            }
        });
        environment.jersey().register(CheckerResource.class);

        // Export all DropWizard metrics to Prometheus, they will be picked up later by the
        // Prometheus MetricsServlet.
        CollectorRegistry.defaultRegistry.register(new DropwizardExports(metrics.getRegistry()));
    }

    @Override
    public void initialize(final Bootstrap<ApplicationConfiguration> bootstrap) {
        super.initialize(bootstrap);
        bootstrap.setConfigurationSourceProvider(new ResourceConfigurationSourceProvider());
        bootstrap.setMetricRegistry(metrics.getRegistry("health-checker"));
    }

    private void registerJvmMetrics() {
        // Register JVM metrics at the top level (no 'health-checker' prefix).
        // This is done only once here, other modules disable them because they would be duplicates.
        MetricRegistry topLevelRegistry = metrics.getRegistry();
        topLevelRegistry.register("jvm.attribute", new JvmAttributeGaugeSet());
        topLevelRegistry.register("jvm.buffers", new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer()));
        topLevelRegistry.register("jvm.classloader", new ClassLoadingGaugeSet());
        topLevelRegistry.register("jvm.filedescriptor", new FileDescriptorRatioGauge());
        topLevelRegistry.register("jvm.gc", new GarbageCollectorMetricSet());
        topLevelRegistry.register("jvm.memory", new MemoryUsageGaugeSet());
        topLevelRegistry.register("jvm.threads", new ThreadStatesGaugeSet());
    }
}
