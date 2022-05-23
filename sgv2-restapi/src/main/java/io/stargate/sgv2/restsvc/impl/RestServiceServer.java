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
package io.stargate.sgv2.restsvc.impl;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.dropwizard.Application;
import io.dropwizard.cli.Cli;
import io.dropwizard.configuration.ResourceConfigurationSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.util.JarLocation;
import io.stargate.bridge.proto.Schema.SchemaRead;
import io.stargate.core.metrics.api.HttpMetricsTagProvider;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.core.metrics.api.MetricsScraper;
import io.stargate.metrics.jersey.MetricsBinder;
import io.stargate.sgv2.common.grpc.StargateBridgeClient;
import io.stargate.sgv2.common.grpc.StargateBridgeClientFactory;
import io.stargate.sgv2.common.http.CreateStargateBridgeClientFilter;
import io.stargate.sgv2.common.http.StargateBridgeClientJerseyFactory;
import io.stargate.sgv2.common.metrics.ApiTimingDiagnosticsFactory;
import io.stargate.sgv2.common.metrics.ApiTimingDiagnosticsSampler;
import io.stargate.sgv2.restsvc.models.RestServiceError;
import io.stargate.sgv2.restsvc.resources.HealthResource;
import io.stargate.sgv2.restsvc.resources.MetricsResource;
import io.stargate.sgv2.restsvc.resources.Sgv2RowsResourceImpl;
import io.stargate.sgv2.restsvc.resources.SwaggerUIResource;
import io.stargate.sgv2.restsvc.resources.schemas.Sgv2ColumnsResourceImpl;
import io.stargate.sgv2.restsvc.resources.schemas.Sgv2IndexesResourceImpl;
import io.stargate.sgv2.restsvc.resources.schemas.Sgv2KeyspacesResourceImpl;
import io.stargate.sgv2.restsvc.resources.schemas.Sgv2TablesResourceImpl;
import io.stargate.sgv2.restsvc.resources.schemas.Sgv2UDTsResourceImpl;
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
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.process.internal.RequestScoped;
import org.glassfish.jersey.server.ServerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** DropWizard {@code Application} that will serve Stargate v2 REST service endpoints. */
public class RestServiceServer extends Application<RestServiceServerConfiguration> {
  public static final String REST_SVC_MODULE_NAME = "sgv2-rest-service";

  /**
   * System property used to configure sampler logic for diagnostics; see {@link
   * ApiTimingDiagnosticsSampler} for details
   */
  public static final String SYSPROP_DIAGNOSTICS_SAMPLER = "stargate.rest.diagnostics.sample";

  /**
   * Instead of relying on name of an arbitrary Java Class or Package, let's use explicit name
   * instead. Could be made configurable if we wanted to, but hard-code at first.
   */
  public static final String LOGGER_NAME_FOR_REST_API_TIMINGS = "stargate.rest.diagnostics";

  public static final String[] NON_API_URI_REGEX = new String[] {"^/$", "^/health$", "^/swagger.*"};

  private static final Logger LOGGER = LoggerFactory.getLogger(RestServiceServer.class);

  private final Metrics metrics;
  private final MetricRegistry metricRegistry;
  private final MetricsScraper metricsScraper;
  private final HttpMetricsTagProvider httpMetricsTagProvider;
  private final int timeoutSeconds;

  public RestServiceServer(
      Metrics metrics,
      MetricsScraper metricsScraper,
      HttpMetricsTagProvider httpMetricsTagProvider,
      int timeoutSeconds) {
    this.metrics = metrics;
    this.metricRegistry = metrics.getRegistry(REST_SVC_MODULE_NAME);
    this.metricsScraper = metricsScraper;
    this.httpMetricsTagProvider = httpMetricsTagProvider;
    this.timeoutSeconds = timeoutSeconds;

    BeanConfig beanConfig = new BeanConfig();
    beanConfig.setSchemes(new String[] {"http"});
    beanConfig.setBasePath("/");
    // Needed for Swagger UI to find endpoints dynamically
    ScannerFactory.setScanner(new DefaultJaxrsScanner());
  }

  /**
   * The only reason we override this is to remove the call to {@code bootstrap.registerMetrics()}.
   *
   * <p>JVM metrics are registered once at the top level in the health-checker module.
   */
  @Override
  public void run(String... arguments) {
    final Bootstrap<RestServiceServerConfiguration> bootstrap = new Bootstrap<>(this);
    addDefaultCommands(bootstrap);
    initialize(bootstrap);

    final Cli cli = new Cli(new JarLocation(getClass()), bootstrap, System.out, System.err);
    // only exit if there's an error running the command
    cli.run(arguments).ifPresent(this::onFatalError);
  }

  @Override
  public void run(final RestServiceServerConfiguration appConfig, final Environment environment)
      throws IOException {

    StargateBridgeClientFactory clientFactory =
        StargateBridgeClientFactory.newInstance(
            appConfig.stargate.bridge.buildChannel(), timeoutSeconds, SchemaRead.SourceApi.REST);
    environment.jersey().register(buildClientFilter(clientFactory));

    environment
        .jersey()
        .register(
            new AbstractBinder() {
              @Override
              protected void configure() {
                bind(configureObjectMapper(environment.getObjectMapper())).to(ObjectMapper.class);
                bind(metricsScraper).to(MetricsScraper.class);
                bindFactory(StargateBridgeClientJerseyFactory.class)
                    .to(StargateBridgeClient.class)
                    .in(RequestScoped.class);
                bind(buildDiagnosticsFactory()).to(ApiTimingDiagnosticsFactory.class);
              }
            });

    // Endpoint, handler registrations:

    environment.jersey().register(new JerseyViolationExceptionMapper());
    environment.jersey().register(new GrpcExceptionMapper());
    environment.jersey().register(new BridgeAuthorizationExceptionMapper());
    environment.jersey().register(new DefaultExceptionMapper());

    // General healthcheck etc endpoints
    environment.jersey().register(HealthResource.class);
    environment.jersey().register(MetricsResource.class);

    // Main data endpoints
    environment.jersey().register(Sgv2RowsResourceImpl.class);

    // Schema endpoints
    environment.jersey().register(Sgv2ColumnsResourceImpl.class);
    environment.jersey().register(Sgv2KeyspacesResourceImpl.class);
    environment.jersey().register(Sgv2TablesResourceImpl.class);
    environment.jersey().register(Sgv2IndexesResourceImpl.class);
    environment.jersey().register(Sgv2UDTsResourceImpl.class);

    // Swagger endpoints
    environment.jersey().register(SwaggerSerializers.class);
    environment.jersey().register(ApiListingResource.class);
    environment.jersey().register(new SwaggerUIResource());

    enableCors(environment);

    final MetricsBinder metricsBinder =
        new MetricsBinder(
            metrics,
            httpMetricsTagProvider,
            REST_SVC_MODULE_NAME,
            Arrays.asList(NON_API_URI_REGEX));
    metricsBinder.register(environment.jersey());

    // no html content
    environment.jersey().property(ServerProperties.RESPONSE_SET_STATUS_OVER_SEND_ERROR, true);
  }

  private CreateStargateBridgeClientFilter buildClientFilter(
      StargateBridgeClientFactory stargateBridgeClientFactory) {
    return new CreateStargateBridgeClientFilter(stargateBridgeClientFactory) {
      @Override
      protected Response buildError(Response.Status status, String message, MediaType mediaType) {
        return Response.status(status)
            .entity(new RestServiceError(message, status.getStatusCode()))
            .build();
      }
    };
  }

  private ApiTimingDiagnosticsFactory buildDiagnosticsFactory() {
    final String samplerDef = System.getProperty(SYSPROP_DIAGNOSTICS_SAMPLER, "");
    ApiTimingDiagnosticsSampler sampler;

    try {
      sampler = ApiTimingDiagnosticsSampler.fromString(samplerDef);
    } catch (IllegalArgumentException e) {
      LOGGER.error(
          "Unrecognized definition for ApiTimingDiagnosticsSampler; will default to 'none' sampler: {}",
          e.getMessage());
      sampler = ApiTimingDiagnosticsSampler.noneSampler();
    }
    LOGGER.info(
        "Constructing ApiTimingDiagnosticsFactory for REST API using sampler: {}",
        sampler.toString());
    return ApiTimingDiagnosticsFactory.createFactory(
        metricRegistry, "", LoggerFactory.getLogger(LOGGER_NAME_FOR_REST_API_TIMINGS), sampler);
  }

  public static ObjectMapper configureObjectMapper(ObjectMapper objectMapper) {
    objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    objectMapper.registerModule(new JavaTimeModule());
    return objectMapper;
  }

  @Override
  public void initialize(final Bootstrap<RestServiceServerConfiguration> bootstrap) {
    super.initialize(bootstrap);
    bootstrap.setConfigurationSourceProvider(new ResourceConfigurationSourceProvider());
    bootstrap.setMetricRegistry(metricRegistry);
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
