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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.dropwizard.Application;
import io.dropwizard.cli.Cli;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.util.JarLocation;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthorizationService;
import io.stargate.core.metrics.api.HttpMetricsTagProvider;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.metrics.jersey.MetricsBinder;
import io.stargate.metrics.jersey.dwconfig.StargateV1ConfigurationSourceProvider;
import io.stargate.web.resources.HealthResource;
import io.stargate.web.resources.SwaggerUIResource;
import io.stargate.web.restapi.dao.RestDBFactory;
import io.stargate.web.restapi.resources.v1.ColumnResource;
import io.stargate.web.restapi.resources.v1.KeyspaceResource;
import io.stargate.web.restapi.resources.v1.RowResource;
import io.stargate.web.restapi.resources.v1.TableResource;
import io.swagger.config.ScannerFactory;
import io.swagger.jaxrs.config.BeanConfig;
import io.swagger.jaxrs.config.DefaultJaxrsScanner;
import io.swagger.jaxrs.listing.ApiListingResource;
import io.swagger.jaxrs.listing.SwaggerSerializers;
import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ServerProperties;
import org.osgi.framework.Bundle;
import org.osgi.framework.FrameworkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DropWizard {@code Application} that will serve Stargate V1 REST, RESTv1 API endpoints.
 *
 * <p>NOTE: used to serve Documents API as well as RESTv2 API endpoints in Stargate v1.
 */
public class RestApiServer extends Application<RestApiServerConfiguration> {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  public static final String[] NON_API_URI_REGEX = new String[] {"^/$", "^/health$", "^/swagger.*"};

  private final AuthenticationService authenticationService;
  private final AuthorizationService authorizationService;
  private final Metrics metrics;
  private final HttpMetricsTagProvider httpMetricsTagProvider;
  private final DataStoreFactory dataStoreFactory;

  public RestApiServer(
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      Metrics metrics,
      HttpMetricsTagProvider httpMetricsTagProvider,
      DataStoreFactory dataStoreFactory) {
    this.authenticationService = authenticationService;
    this.authorizationService = authorizationService;
    this.metrics = metrics;
    this.httpMetricsTagProvider = httpMetricsTagProvider;
    this.dataStoreFactory = dataStoreFactory;

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
    final Bootstrap<RestApiServerConfiguration> bootstrap = new Bootstrap<>(this);
    addDefaultCommands(bootstrap);
    initialize(bootstrap);

    final Cli cli = new Cli(new JarLocation(getClass()), bootstrap, System.out, System.err);
    // only exit if there's an error running the command
    cli.run(arguments).ifPresent(this::onFatalError);
  }

  @Override
  public void run(
      final RestApiServerConfiguration applicationConfiguration, final Environment environment)
      throws IOException {

    // General providers
    environment.jersey().register(new JerseyViolationExceptionMapper());
    final RestDBFactory restDBFactory =
        new RestDBFactory(authenticationService, authorizationService, dataStoreFactory);
    final ObjectMapper objectMapper = configureObjectMapper(environment.getObjectMapper());
    environment
        .jersey()
        .register(
            new AbstractBinder() {
              @Override
              protected void configure() {
                bind(objectMapper).to(ObjectMapper.class);
                bind(restDBFactory).to(RestDBFactory.class);
              }
            });

    // General healthcheck endpoint
    environment.jersey().register(HealthResource.class);

    // Always enable RESTv1 endpoints

    logger.info("Registering StargateV1 RESTv1 endpoint for StargateV2");
    environment.jersey().register(ColumnResource.class);
    environment.jersey().register(KeyspaceResource.class);
    environment.jersey().register(RowResource.class);
    environment.jersey().register(TableResource.class);

    // Swagger endpoints
    environment
        .jersey()
        .register(
            new AbstractBinder() {
              @Override
              protected void configure() {
                bind(FrameworkUtil.getBundle(RestApiActivator.class)).to(Bundle.class);
              }
            });
    environment.jersey().register(SwaggerSerializers.class);
    environment.jersey().register(ApiListingResource.class);
    environment.jersey().register(SwaggerUIResource.class);

    enableCors(environment);

    MetricsBinder metricsBinder =
        new MetricsBinder(
            metrics,
            httpMetricsTagProvider,
            RestApiActivator.MODULE_NAME,
            Arrays.asList(NON_API_URI_REGEX));
    metricsBinder.register(environment.jersey());

    // no html content
    environment.jersey().property(ServerProperties.RESPONSE_SET_STATUS_OVER_SEND_ERROR, true);
  }

  public static ObjectMapper configureObjectMapper(ObjectMapper objectMapper) {
    objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    objectMapper.registerModule(new JavaTimeModule());
    return objectMapper;
  }

  @Override
  public void initialize(final Bootstrap<RestApiServerConfiguration> bootstrap) {
    super.initialize(bootstrap);
    bootstrap.setConfigurationSourceProvider(
        new StargateV1ConfigurationSourceProvider(RestApiActivator.MODULE_NAME));
    bootstrap.setMetricRegistry(metrics.getRegistry(RestApiActivator.MODULE_NAME));
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

  @Override
  protected void bootstrapLogging() {
    // disable dropwizard logging, it will use external logback
  }
}
