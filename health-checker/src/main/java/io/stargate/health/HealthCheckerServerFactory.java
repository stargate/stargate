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
package io.stargate.health;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.codahale.metrics.servlets.HealthCheckServlet;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dropwizard.jersey.filter.AllowedMethodsFilter;
import io.dropwizard.jetty.MutableServletContextHandler;
import io.dropwizard.jetty.NonblockingServletHolder;
import io.dropwizard.server.DefaultServerFactory;
import java.util.EnumSet;
import java.util.stream.Collectors;
import javax.servlet.DispatcherType;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;

/**
 * Custom DropWizard server factory, in order to plug our {@link HealthCheckerAdminServlet}.
 *
 * <p>This class is made available to DropWizard via {@code
 * META-INF/services/io.dropwizard.server.ServerFactory}, and then enabled via the {@code
 * server.type} option in {@code config.yaml}.
 */
@JsonTypeName("health-checker")
public class HealthCheckerServerFactory extends DefaultServerFactory {

  @Override
  protected Handler createAdminServlet(
      Server server,
      MutableServletContextHandler handler,
      MetricRegistry metrics,
      HealthCheckRegistry healthChecks) {
    configureSessionsAndSecurity(handler, server);
    handler.setServer(server);
    handler
        .getServletContext()
        .setAttribute(HealthCheckServlet.HEALTH_CHECK_REGISTRY, healthChecks);
    handler.addServlet(new NonblockingServletHolder(new HealthCheckerAdminServlet()), "/*");
    final String allowedMethodsParam =
        getAllowedMethods().stream().collect(Collectors.joining(","));
    handler
        .addFilter(AllowedMethodsFilter.class, "/*", EnumSet.of(DispatcherType.REQUEST))
        .setInitParameter(AllowedMethodsFilter.ALLOWED_METHODS_PARAM, allowedMethodsParam);
    return handler;
  }

  private void configureSessionsAndSecurity(MutableServletContextHandler handler, Server server) {
    handler.setServer(server);
    if (handler.isSecurityEnabled()) {
      handler.getSecurityHandler().setServer(server);
    }
    if (handler.isSessionsEnabled()) {
      handler.getSessionHandler().setServer(server);
    }
  }
}
