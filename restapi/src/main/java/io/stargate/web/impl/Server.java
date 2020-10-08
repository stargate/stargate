/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.web.impl;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.dropwizard.Application;
import io.dropwizard.cli.Cli;
import io.dropwizard.configuration.ResourceConfigurationSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.util.JarLocation;
import io.stargate.auth.AuthenticationService;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.db.Persistence;
import io.stargate.web.RestApiActivator;
import io.stargate.web.config.ApplicationConfiguration;
import io.stargate.web.docsapi.resources.DocumentResourceV2;
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
import io.stargate.web.swagger.SwaggerUIResource;
import io.swagger.config.ScannerFactory;
import io.swagger.jaxrs.config.BeanConfig;
import io.swagger.jaxrs.config.DefaultJaxrsScanner;
import io.swagger.jaxrs.listing.ApiListingResource;
import io.swagger.jaxrs.listing.SwaggerSerializers;
import java.util.EnumSet;
import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.osgi.framework.Bundle;
import org.osgi.framework.FrameworkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Server extends Application<ApplicationConfiguration> {
  private static final Logger logger = LoggerFactory.getLogger(Server.class);

  Persistence persistence;
  AuthenticationService authenticationService;
  private final Metrics metrics;

  public Server(
      Persistence persistence, AuthenticationService authenticationService, Metrics metrics) {
    this.persistence = persistence;
    this.authenticationService = authenticationService;
    this.metrics = metrics;

    BeanConfig beanConfig = new BeanConfig();
    beanConfig.setSchemes(new String[] {"http"});
    beanConfig.setBasePath("/");
    ScannerFactory.setScanner(new DefaultJaxrsScanner());
  }

  /**
   * The only reason we override this is to remove the call to {@code bootstrap.registerMetrics()}.
   *
   * <p>JVM metrics are registered once at the top level in the health-checker module.
   */
  @Override
  public void run(String... arguments) {
    final Bootstrap<ApplicationConfiguration> bootstrap = new Bootstrap<>(this);
    addDefaultCommands(bootstrap);
    initialize(bootstrap);

    final Cli cli = new Cli(new JarLocation(getClass()), bootstrap, System.out, System.err);
    // only exit if there's an error running the command
    cli.run(arguments).ifPresent(this::onFatalError);
  }

  @Override
  public void run(
      final ApplicationConfiguration applicationConfiguration, final Environment environment) {
    final Db db = new Db(persistence, authenticationService);

    environment.getObjectMapper().configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    environment.getObjectMapper().registerModule(new JavaTimeModule());

    environment
        .jersey()
        .register(
            new AbstractBinder() {
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

    // Documents API
    environment.jersey().register(DocumentResourceV2.class);

    environment.jersey().register(ApiListingResource.class);
    environment.jersey().register(SwaggerSerializers.class);

    environment
        .jersey()
        .register(
            new AbstractBinder() {
              @Override
              protected void configure() {
                bind(FrameworkUtil.getBundle(RestApiActivator.class)).to(Bundle.class);
              }
            });

    environment.jersey().register(SwaggerUIResource.class);

    enableCors(environment);
  }

  @Override
  public void initialize(final Bootstrap<ApplicationConfiguration> bootstrap) {
    super.initialize(bootstrap);
    bootstrap.setConfigurationSourceProvider(new ResourceConfigurationSourceProvider());
    bootstrap.setMetricRegistry(metrics.getRegistry("restapi"));
  }

  private void enableCors(Environment environment) {
    FilterRegistration.Dynamic filter =
        environment.servlets().addFilter("cors", CrossOriginFilter.class);

    filter.setInitParameter(
        CrossOriginFilter.ALLOWED_METHODS_PARAM, "POST,GET,OPTIONS,PUT,DELETE,PATCH");
    filter.setInitParameter(CrossOriginFilter.ALLOWED_ORIGINS_PARAM, "*");
    filter.setInitParameter(CrossOriginFilter.ACCESS_CONTROL_ALLOW_ORIGIN_HEADER, "*");
    filter.setInitParameter(CrossOriginFilter.ALLOWED_HEADERS_PARAM, "*");
    filter.setInitParameter(CrossOriginFilter.ALLOW_CREDENTIALS_PARAM, "true");
    filter.setInitParameter(CrossOriginFilter.EXPOSED_HEADERS_PARAM, "Date");

    filter.addMappingForUrlPatterns(EnumSet.allOf(DispatcherType.class), true, "/*");
  }
}
