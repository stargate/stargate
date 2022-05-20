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
package io.stargate.sgv2.graphql.impl;

import com.codahale.metrics.MetricRegistry;
import io.dropwizard.Application;
import io.dropwizard.configuration.ResourceConfigurationSourceProvider;
import io.dropwizard.forms.MultiPartBundle;
import io.dropwizard.jersey.setup.JerseyEnvironment;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.stargate.bridge.proto.Schema;
import io.stargate.core.metrics.api.HttpMetricsTagProvider;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.core.metrics.api.MetricsScraper;
import io.stargate.sgv2.common.grpc.StargateBridgeClient;
import io.stargate.sgv2.common.grpc.StargateBridgeClientFactory;
import io.stargate.sgv2.common.http.CreateStargateBridgeClientFilter;
import io.stargate.sgv2.common.http.StargateBridgeClientJerseyFactory;
import io.stargate.sgv2.graphql.resources.HealthResource;
import io.stargate.sgv2.graphql.resources.MetricsResource;
import io.stargate.sgv2.graphql.web.resources.AdminResource;
import io.stargate.sgv2.graphql.web.resources.DdlResource;
import io.stargate.sgv2.graphql.web.resources.DmlResource;
import io.stargate.sgv2.graphql.web.resources.FilesResource;
import io.stargate.sgv2.graphql.web.resources.GraphqlCache;
import io.stargate.sgv2.graphql.web.resources.PlaygroundResource;
import java.util.Collections;
import java.util.EnumSet;
import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.glassfish.hk2.api.PerLookup;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

public class GraphqlServiceServer extends Application<GraphqlServiceServerConfiguration> {

  public static final String MODULE_NAME = "sgv2-graphqlapi";

  private final Metrics metrics;
  private final MetricRegistry metricRegistry;
  private final MetricsScraper metricsScraper;
  private final HttpMetricsTagProvider httpMetricsTagProvider;
  private final int timeoutSeconds;
  private final boolean disablePlayground;
  private final boolean disableDefaultKeyspace;

  public GraphqlServiceServer(
      Metrics metrics,
      MetricsScraper metricsScraper,
      HttpMetricsTagProvider httpMetricsTagProvider,
      int timeoutSeconds,
      boolean enableGraphqlPlayground,
      boolean disableDefaultKeyspace) {
    this.metrics = metrics;
    this.metricRegistry = metrics.getRegistry(MODULE_NAME);
    this.metricsScraper = metricsScraper;
    this.httpMetricsTagProvider = httpMetricsTagProvider;
    this.timeoutSeconds = timeoutSeconds;
    this.disablePlayground = enableGraphqlPlayground;
    this.disableDefaultKeyspace = disableDefaultKeyspace;
  }

  @Override
  public void run(GraphqlServiceServerConfiguration config, Environment environment) {

    JerseyEnvironment jersey = environment.jersey();

    StargateBridgeClientFactory clientFactory =
        StargateBridgeClientFactory.newInstance(
            config.stargate.bridge.buildChannel(),
            timeoutSeconds,
            Schema.SchemaRead.SourceApi.GRAPHQL);
    jersey.register(buildClientFilter(clientFactory));

    GraphqlCache graphqlCache = new GraphqlCache(disableDefaultKeyspace);

    jersey.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(metricsScraper).to(MetricsScraper.class);
            bindFactory(StargateBridgeClientJerseyFactory.class)
                .to(StargateBridgeClient.class)
                // Note: this should really be `RequestScoped.class`, but using that causes HK2
                // errors.
                // In practice that won't make a difference because our resources look up the client
                // at most once.
                .in(PerLookup.class);
            bind(graphqlCache).to(GraphqlCache.class);
          }
        });
    environment.jersey().register(DdlResource.class);
    environment.jersey().register(DmlResource.class);
    environment.jersey().register(AdminResource.class);
    environment.jersey().register(FilesResource.class);

    if (!disablePlayground) {
      environment.jersey().register(PlaygroundResource.class);
    }

    environment.jersey().register(HealthResource.class);
    environment.jersey().register(MetricsResource.class);

    enableCors(environment);
  }

  private CreateStargateBridgeClientFilter buildClientFilter(
      StargateBridgeClientFactory clientFactory) {
    return new CreateStargateBridgeClientFilter(clientFactory) {
      @Override
      protected Response buildError(Response.Status status, String message, MediaType mediaType) {
        Object entity =
            mediaType != null
                    && ("json".equals(mediaType.getSubtype())
                        || "graphql".equals(mediaType.getSubtype()))
                ? Collections.singletonMap("errors", Collections.singletonList(message))
                : message;
        return Response.status(status).entity(entity).build();
      }
    };
  }

  @Override
  public void initialize(Bootstrap<GraphqlServiceServerConfiguration> bootstrap) {
    super.initialize(bootstrap);
    bootstrap.setConfigurationSourceProvider(new ResourceConfigurationSourceProvider());
    bootstrap.setMetricRegistry(metricRegistry);
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

  @Override
  protected void bootstrapLogging() {
    // disable dropwizard logging, it will use external logback
  }
}
