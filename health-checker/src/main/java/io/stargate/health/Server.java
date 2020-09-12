package io.stargate.health;

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
    }

    @Override
    public void initialize(final Bootstrap<ApplicationConfiguration> bootstrap) {
        super.initialize(bootstrap);
        bootstrap.setConfigurationSourceProvider(new ResourceConfigurationSourceProvider());
        bootstrap.setMetricRegistry(metrics.getRegistry());
    }
}
