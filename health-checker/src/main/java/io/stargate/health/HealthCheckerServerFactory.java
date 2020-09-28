package io.stargate.health;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.codahale.metrics.servlets.HealthCheckServlet;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dropwizard.jersey.filter.AllowedMethodsFilter;
import io.dropwizard.jetty.MutableServletContextHandler;
import io.dropwizard.jetty.NonblockingServletHolder;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.server.DefaultServerFactory;
import java.util.EnumSet;
import java.util.stream.Collectors;
import javax.servlet.DispatcherType;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.thread.ThreadPool;

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

  @Override
  protected Server buildServer(LifecycleEnvironment lifecycle, ThreadPool threadPool) {
    System.out.println("setting ServerConnector");
    Server server = super.buildServer(lifecycle, threadPool);
    ServerConnector connector = new ServerConnector(server);
    if (Boolean.parseBoolean(System.getProperty("stargate.bind_to_listen_address"))) {
      connector.setHost(System.getProperty("stargate.listen_address"));
    }
    connector.setPort(8085);
    server.addConnector(connector);
    return server;
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
