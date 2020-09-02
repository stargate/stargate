package io.stargate.web.impl;

import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.dropwizard.Application;
import io.dropwizard.configuration.ResourceConfigurationSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.stargate.auth.AuthenticationService;
import io.stargate.db.Persistence;
import io.stargate.web.config.ApplicationConfiguration;
import io.stargate.web.resources.ColumnResource;
import io.stargate.web.resources.Db;
import io.stargate.web.resources.HealthResource;
import io.stargate.web.resources.KeyspaceResource;
import io.stargate.web.resources.RowResource;
import io.stargate.web.resources.TableResource;
import io.stargate.web.resources.v2.RowsResource;
import io.stargate.web.resources.v2.schemas.ColumnsResource;
import io.stargate.web.resources.v2.schemas.KeyspacesResource;
import io.stargate.web.resources.v2.schemas.TablesResource;

public class Server extends Application<ApplicationConfiguration> {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    Persistence persistence;
    AuthenticationService authenticationService;

    public Server(Persistence persistence, AuthenticationService authenticationService) {
        this.persistence = persistence;
        this.authenticationService = authenticationService;
    }

    @Override
    public void run(final ApplicationConfiguration applicationConfiguration, final Environment environment) {
        final Db db = new Db(persistence, authenticationService);

        environment.getObjectMapper().configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        environment.getObjectMapper().registerModule(new JavaTimeModule());

        environment.jersey().register(new AbstractBinder() {
            @Override
            protected void configure() {
                bind(db).to(Db.class);
            }
        });
        environment.jersey().register(KeyspaceResource.class);
        environment.jersey().register(TableResource.class);
        environment.jersey().register(RowResource.class);
        environment.jersey().register(ColumnResource.class);
        environment.jersey().register(HealthResource.class);
        environment.jersey().register(RowsResource.class);
        environment.jersey().register(TablesResource.class);
        environment.jersey().register(KeyspacesResource.class);
        environment.jersey().register(ColumnsResource.class);
//        environment.jersey().register((ExceptionMapper<Throwable>) t -> {
//            logger.error("Server error occurred", t);
//            return Response.serverError()
//                    .entity("Internal server error: " + t.getMessage())
//                    .build();
//        });
    }

    @Override
    public void initialize(final Bootstrap<ApplicationConfiguration> bootstrap) {
        super.initialize(bootstrap);
        bootstrap.setConfigurationSourceProvider(new ResourceConfigurationSourceProvider());
    }
}
