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
package io.stargate.graphql.web;

import io.dropwizard.Application;
import io.dropwizard.Configuration;
import io.dropwizard.cli.Cli;
import io.dropwizard.configuration.ResourceConfigurationSourceProvider;
import io.dropwizard.forms.MultiPartBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.util.JarLocation;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthorizationService;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.db.Persistence;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.graphql.GraphqlActivator;
import io.stargate.graphql.web.resources.PlaygroundResource;
import io.stargate.graphql.web.resources.cqlfirst.GraphqlCache;
import io.stargate.graphql.web.resources.cqlfirst.GraphqlDdlResource;
import io.stargate.graphql.web.resources.cqlfirst.GraphqlDmlResource;
import io.stargate.graphql.web.resources.schemafirst.AdminResource;
import io.stargate.graphql.web.resources.schemafirst.FilesResource;
import io.stargate.graphql.web.resources.schemafirst.NamespaceResource;
import io.stargate.graphql.web.resources.schemafirst.SchemaFirstCache;
import java.util.EnumSet;
import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.osgi.framework.Bundle;
import org.osgi.framework.FrameworkUtil;

public class DropwizardServer extends Application<Configuration> {

  private final Persistence persistence;
  private final AuthenticationService authenticationService;
  private final AuthorizationService authorizationService;
  private final DataStoreFactory dataStoreFactory;
  private final Metrics metrics;
  private volatile Server jettyServer;

  public DropwizardServer(
      Persistence persistence,
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      Metrics metrics,
      DataStoreFactory dataStoreFactory) {
    this.persistence = persistence;
    this.authenticationService = authenticationService;
    this.authorizationService = authorizationService;
    this.metrics = metrics;
    this.dataStoreFactory = dataStoreFactory;
  }

  /**
   * The only reason we override this is to remove the call to {@code bootstrap.registerMetrics()}.
   *
   * <p>JVM metrics are registered once at the top level in the health-checker module.
   */
  @Override
  public void run(String... arguments) {
    final Bootstrap<Configuration> bootstrap = new Bootstrap<>(this);
    addDefaultCommands(bootstrap);
    initialize(bootstrap);

    final Cli cli = new Cli(new JarLocation(getClass()), bootstrap, System.out, System.err);
    // only exit if there's an error running the command
    cli.run(arguments).ifPresent(this::onFatalError);
  }

  @Override
  public void run(final Configuration config, final Environment environment) throws Exception {

    GraphqlCache graphqlCache =
        new GraphqlCache(
            persistence, authenticationService, authorizationService, dataStoreFactory);
    environment
        .jersey()
        .register(
            new AbstractBinder() {
              @Override
              protected void configure() {
                bind(graphqlCache).to(GraphqlCache.class);
              }
            });

    SchemaFirstCache schemaFirstCache =
        new SchemaFirstCache(
            persistence, authenticationService, authorizationService, dataStoreFactory);
    environment
        .jersey()
        .register(
            new AbstractBinder() {
              @Override
              protected void configure() {
                bind(schemaFirstCache).to(SchemaFirstCache.class);
              }
            });

    environment
        .jersey()
        .register(
            new AbstractBinder() {
              @Override
              protected void configure() {
                bind(FrameworkUtil.getBundle(GraphqlActivator.class)).to(Bundle.class);
              }
            });

    environment.jersey().register(PlaygroundResource.class);

    // CQL-first API:
    environment.jersey().register(GraphqlDmlResource.class);
    environment.jersey().register(GraphqlDdlResource.class);

    // GraphQL-first API:
    environment.jersey().register(AdminResource.class);
    environment.jersey().register(NamespaceResource.class);
    environment.jersey().register(FilesResource.class);

    enableCors(environment);

    environment
        .lifecycle()
        .addServerLifecycleListener(server -> DropwizardServer.this.jettyServer = server);
  }

  @Override
  public void initialize(final Bootstrap<Configuration> bootstrap) {
    super.initialize(bootstrap);
    bootstrap.setConfigurationSourceProvider(new ResourceConfigurationSourceProvider());
    bootstrap.setMetricRegistry(metrics.getRegistry("graphqlapi"));
    bootstrap.addBundle(new MultiPartBundle());
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

  public void stop() throws Exception {
    Server s = this.jettyServer;
    if (s != null) {
      s.stop();
    }
  }
}
