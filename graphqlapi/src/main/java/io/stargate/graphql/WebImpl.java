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

import graphql.kickstart.servlet.CustomGraphQLServlet;
import graphql.kickstart.servlet.SchemaGraphQLServlet;
import io.stargate.auth.AuthenticationService;
import io.stargate.db.Persistence;
import java.util.EnumSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.DispatcherType;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlets.CrossOriginFilter;

public class WebImpl {
  private Server server;
  private Persistence persistence;
  private AuthenticationService authenticationService;

  public void start() throws Exception {
    server = new Server();

    ServerConnector connector = new ServerConnector(server);
    connector.setHost(System.getProperty("stargate.listen_address"));
    connector.setPort(8080);
    server.addConnector(connector);

    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
    context.setContextPath("/");
    ServletHolder servletHolder =
        new ServletHolder(new CustomGraphQLServlet(persistence, authenticationService));
    context.addServlet(servletHolder, "/graphql/*");
    ServletHolder schema =
        new ServletHolder(new SchemaGraphQLServlet(persistence, authenticationService));
    context.addServlet(schema, "/graphql-schema");

    ServletHolder playground = new ServletHolder(new PlaygroundServlet());
    context.addServlet(playground, "/playground");

    FilterHolder filter = new FilterHolder();
    filter.setInitParameter(
        CrossOriginFilter.ALLOWED_METHODS_PARAM, "POST,GET,OPTIONS,PUT,DELETE,PATCH");
    filter.setInitParameter(CrossOriginFilter.ALLOWED_ORIGINS_PARAM, "*");
    filter.setInitParameter(CrossOriginFilter.ACCESS_CONTROL_ALLOW_ORIGIN_HEADER, "*");
    filter.setInitParameter(CrossOriginFilter.ALLOWED_HEADERS_PARAM, "*");
    filter.setInitParameter(CrossOriginFilter.ALLOW_CREDENTIALS_PARAM, "true");
    filter.setInitParameter(CrossOriginFilter.EXPOSED_HEADERS_PARAM, "Date");
    CrossOriginFilter corsFilter = new CrossOriginFilter();
    filter.setFilter(corsFilter);

    context.addFilter(
        org.eclipse.jetty.servlets.CrossOriginFilter.class,
        "/*",
        EnumSet.allOf(DispatcherType.class));

    server.setHandler(context);

    try {
      server.start();
      server.dump(System.err);
    } catch (Exception ex) {
      Logger.getLogger(WebImpl.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  public void stop() throws Exception {
    if (server != null) {
      server.stop();
    }
  }

  public void setPersistence(Persistence persistence) {
    this.persistence = persistence;
  }

  public Persistence getPersistence() {
    return persistence;
  }

  public AuthenticationService getAuthenticationService() {
    return authenticationService;
  }

  public void setAuthenticationService(AuthenticationService authenticationService) {
    this.authenticationService = authenticationService;
  }
}
