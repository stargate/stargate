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
package io.stargate.sgv2.dynamosvc.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.dropwizard.Application;
import io.dropwizard.cli.Cli;
import io.dropwizard.configuration.ResourceConfigurationSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.util.JarLocation;
import io.grpc.ManagedChannel;
import io.stargate.core.grpc.BridgeConfig;
import io.stargate.core.metrics.api.HttpMetricsTagProvider;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.core.metrics.api.MetricsScraper;
import io.stargate.metrics.jersey.MetricsBinder;
import io.stargate.proto.Schema.SchemaRead;
import io.stargate.sgv2.common.grpc.StargateBridgeClient;
import io.stargate.sgv2.common.grpc.StargateBridgeClientFactory;
import io.stargate.sgv2.common.http.CreateStargateBridgeClientFilter;
import io.stargate.sgv2.common.http.StargateBridgeClientJerseyFactory;
import io.stargate.sgv2.dynamosvc.dynamo.ItemProxy;
import io.stargate.sgv2.dynamosvc.dynamo.TableProxy;
import io.stargate.sgv2.dynamosvc.models.DynamoServiceError;
import io.stargate.sgv2.dynamosvc.resources.DynamoResource;
import io.stargate.sgv2.dynamosvc.resources.HealthResource;
import io.stargate.sgv2.dynamosvc.resources.MetricsResource;
import io.stargate.sgv2.dynamosvc.resources.SwaggerUIResource;
import io.swagger.config.ScannerFactory;
import io.swagger.jaxrs.config.BeanConfig;
import io.swagger.jaxrs.config.DefaultJaxrsScanner;
import io.swagger.jaxrs.listing.ApiListingResource;
import io.swagger.jaxrs.listing.SwaggerSerializers;
import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.process.internal.RequestScoped;
import org.glassfish.jersey.server.ServerProperties;

/** DropWizard {@code Application} that will serve Stargate v2 Dynamo service endpoints. */
public class DynamoServiceServer extends Application<DynamoServiceServerConfiguration> {
  public static final String DYNAMO_SVC_MODULE_NAME = "sgv2-dynamo-service";

  public static final String[] NON_API_URI_REGEX = new String[] {"^/$", "^/health$", "^/swagger.*"};
  private static final Pattern CREDENTIAL_PATTERN =
      Pattern.compile("Credential=([^/]+)/[^/]+/[^/]+/dynamodb");

  private final Metrics metrics;
  private final MetricsScraper metricsScraper;
  private final HttpMetricsTagProvider httpMetricsTagProvider;

  public DynamoServiceServer(
      Metrics metrics,
      MetricsScraper metricsScraper,
      HttpMetricsTagProvider httpMetricsTagProvider) {
    this.metrics = metrics;
    this.metricsScraper = metricsScraper;
    this.httpMetricsTagProvider = httpMetricsTagProvider;

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
    final Bootstrap<DynamoServiceServerConfiguration> bootstrap = new Bootstrap<>(this);
    addDefaultCommands(bootstrap);
    initialize(bootstrap);

    final Cli cli = new Cli(new JarLocation(getClass()), bootstrap, System.out, System.err);
    // only exit if there's an error running the command
    cli.run(arguments).ifPresent(this::onFatalError);
  }

  @Override
  public void run(final DynamoServiceServerConfiguration appConfig, final Environment environment)
      throws IOException {

    StargateBridgeClientFactory clientFactory =
        buildClientFactory(appConfig.stargate.bridge.buildChannel(), environment);
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
    environment.jersey().register(new DynamoResource(new TableProxy(), new ItemProxy()));

    // Swagger endpoints
    environment.jersey().register(SwaggerSerializers.class);
    environment.jersey().register(ApiListingResource.class);
    environment.jersey().register(new SwaggerUIResource());

    enableCors(environment);

    final MetricsBinder metricsBinder =
        new MetricsBinder(
            metrics,
            httpMetricsTagProvider,
            DYNAMO_SVC_MODULE_NAME,
            Arrays.asList(NON_API_URI_REGEX));
    metricsBinder.register(environment.jersey());

    // no html content
    environment.jersey().property(ServerProperties.RESPONSE_SET_STATUS_OVER_SEND_ERROR, true);
  }

  private StargateBridgeClientFactory buildClientFactory(
      ManagedChannel channel, Environment environment) {
    return StargateBridgeClientFactory.newInstance(
        channel,
        BridgeConfig.ADMIN_TOKEN,
        SchemaRead.SourceApi.REST,
        environment.lifecycle().scheduledExecutorService("bridge-factory").threads(1).build());
  }

  private CreateStargateBridgeClientFilter buildClientFilter(
      StargateBridgeClientFactory stargateBridgeClientFactory) {
    return new CreateStargateBridgeClientFilter(stargateBridgeClientFactory) {
      @Override
      protected Response buildError(Response.Status status, String message, MediaType mediaType) {
        return Response.status(status)
            .entity(new DynamoServiceError(message, status.getStatusCode()))
            .build();
      }

      @Override
      protected String getToken(ContainerRequestContext context) {
        String credential = context.getHeaderString("Authorization");
        Matcher m = CREDENTIAL_PATTERN.matcher(credential);
        String token = null;
        if (m.find()) {
          // retrieve Stargate token from AWS-generated authorization string
          token = m.group(1);
        } else {
          // normal Stargate token
          token = credential;
        }
        return token;
      }
    };
  }

  public static ObjectMapper configureObjectMapper(ObjectMapper objectMapper) {
    objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    objectMapper.registerModule(new JavaTimeModule());
    return objectMapper;
  }

  @Override
  public void initialize(final Bootstrap<DynamoServiceServerConfiguration> bootstrap) {
    super.initialize(bootstrap);
    bootstrap.setConfigurationSourceProvider(new ResourceConfigurationSourceProvider());
    bootstrap.setMetricRegistry(metrics.getRegistry(DYNAMO_SVC_MODULE_NAME));
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
