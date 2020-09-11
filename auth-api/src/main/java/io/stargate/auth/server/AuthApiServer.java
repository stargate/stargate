package io.stargate.auth.server;

import javax.servlet.DispatcherType;
import java.util.EnumSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.slf4j.LoggerFactory;

import io.stargate.auth.AuthenticationService;
import io.stargate.auth.api.AuthResource;
import io.stargate.auth.api.ErrorHandler;

public class AuthApiServer {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(AuthApiServer.class);

    private Server server;
    private AuthenticationService authService;

    public void setAuthService(AuthenticationService authService) {
        this.authService = authService;
    }

    public AuthenticationService getAuthService() {
        return authService;
    }

    public void start() {
        if (server != null && server.isRunning()) {
            log.info("Returning early from start() since server is running");
            return;
        }

        final ResourceConfig application = new ResourceConfig()
                .register(new AuthResource(this.authService))
                .register(JacksonFeature.class);

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        context.setErrorHandler(new ErrorHandler());

        server = new Server();
        ServerConnector connector = new ServerConnector(server);

        if (Boolean.parseBoolean(System.getProperty("stargate.bind_to_listen_address")))
        {
            connector.setHost(System.getProperty("stargate.listen_address"));
        }

        // TODO: [doug] 2020-06-18, Thu, 0:48 make port configurable
        connector.setPort(8081);
        server.addConnector(connector);

        server.setHandler(context);
        server.addBean(new ErrorHandler());

        HandlerCollection handlers = new HandlerCollection();
        handlers.setHandlers(new Handler[]{context});
        server.setHandler(handlers);

        ServletHolder jerseyServlet = new ServletHolder(new org.glassfish.jersey.servlet.ServletContainer(application));
        jerseyServlet.setInitOrder(0);
        jerseyServlet.setInitParameter(ServerProperties.PROVIDER_PACKAGES, "io.stargate.api");

        context.addServlet(jerseyServlet, "/*");

        FilterHolder filter = new FilterHolder();
        filter.setInitParameter(CrossOriginFilter.ALLOWED_METHODS_PARAM, "POST,GET,OPTIONS,PUT,DELETE,PATCH");
        filter.setInitParameter(CrossOriginFilter.ALLOWED_ORIGINS_PARAM, "*");
        filter.setInitParameter(CrossOriginFilter.ACCESS_CONTROL_ALLOW_ORIGIN_HEADER, "*");
        filter.setInitParameter(CrossOriginFilter.ALLOWED_HEADERS_PARAM, "*");
        filter.setInitParameter(CrossOriginFilter.ALLOW_CREDENTIALS_PARAM, "true");
        filter.setInitParameter(CrossOriginFilter.EXPOSED_HEADERS_PARAM, "Date");
        CrossOriginFilter corsFilter = new CrossOriginFilter();
        filter.setFilter(corsFilter);

        context.addFilter(org.eclipse.jetty.servlets.CrossOriginFilter.class, "/*", EnumSet.allOf(DispatcherType.class));

        try {
            server.start();
        } catch (Exception ex) {
            Logger.getLogger(AuthApiServer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    void stop() throws Exception {
        server.stop();
    }
}
