package io.stargate.auth.server;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;

import io.stargate.auth.AuthenticationService;
import io.stargate.auth.api.AuthResource;
import io.stargate.auth.api.ErrorHandler;

public class AuthApiServer {

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
            System.out.println("Returning early from start() since server is running");
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
        connector.setHost(System.getProperty("stargate.listen_address"));
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
