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
package io.stargate.graphql;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.servlet.InstrumentedFilter;
import graphql.kickstart.servlet.CustomGraphQLServlet;
import graphql.kickstart.servlet.SchemaGraphQLServlet;
import io.stargate.auth.AuthenticationService;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.db.Persistence;
import java.util.EnumSet;
import javax.servlet.DispatcherType;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlets.CrossOriginFilter;

public class WebImpl {

  private final Server server;

  public WebImpl(Persistence persistence, Metrics metrics, AuthenticationService authentication) {
    server = new Server();

    ServerConnector connector = new ServerConnector(server);
    connector.setHost(System.getProperty("stargate.listen_address"));
    connector.setPort(8080);
    server.addConnector(connector);

    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
    context.setContextPath("/");
    ServletHolder servletHolder =
        new ServletHolder(new CustomGraphQLServlet(persistence, authentication));
    context.addServlet(servletHolder, "/graphql/*");
    ServletHolder schema = new ServletHolder(new SchemaGraphQLServlet(persistence, authentication));
    context.addServlet(schema, "/graphql-schema");

    ServletHolder playground = new ServletHolder(new PlaygroundServlet());
    context.addServlet(playground, "/playground");

    EnumSet<DispatcherType> allDispatcherTypes = EnumSet.allOf(DispatcherType.class);

    FilterHolder corsFilter = new FilterHolder();
    corsFilter.setInitParameter(
        CrossOriginFilter.ALLOWED_METHODS_PARAM, "POST,GET,OPTIONS,PUT,DELETE,PATCH");
    corsFilter.setInitParameter(CrossOriginFilter.ALLOWED_ORIGINS_PARAM, "*");
    corsFilter.setInitParameter(CrossOriginFilter.ACCESS_CONTROL_ALLOW_ORIGIN_HEADER, "*");
    corsFilter.setInitParameter(CrossOriginFilter.ALLOWED_HEADERS_PARAM, "*");
    corsFilter.setInitParameter(CrossOriginFilter.ALLOW_CREDENTIALS_PARAM, "true");
    corsFilter.setInitParameter(CrossOriginFilter.EXPOSED_HEADERS_PARAM, "Date");
    corsFilter.setFilter(new CrossOriginFilter());
    context.addFilter(corsFilter, "/*", allDispatcherTypes);

    context
        .addFilter(InstrumentedFilter.class, "/graphql/*", allDispatcherTypes)
        .setInitParameter("name-prefix", "io.stargate.GraphQL");
    context
        .addFilter(InstrumentedFilter.class, "/graphql-schema/*", allDispatcherTypes)
        .setInitParameter("name-prefix", "io.stargate.GraphQLSchema");

    MetricRegistry metricRegistry = metrics.getRegistry("graphql");
    context.setAttribute(InstrumentedFilter.REGISTRY_ATTRIBUTE, metricRegistry);

    server.setHandler(context);
  }

  public void start() throws Exception {
    server.start();
  }

  public void stop() throws Exception {
    if (server != null) {
      server.stop();
    }
  }
}
