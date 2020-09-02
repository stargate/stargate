package io.stargate.graphql;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import io.stargate.auth.AuthenticationService;
import io.stargate.coordinator.Coordinator;
import graphql.kickstart.servlet.CustomGraphQLServlet;
import graphql.kickstart.servlet.SchemaGraphQLServlet;

public class WebImpl {
    private Server server;
    private Coordinator coordinator;
    private AuthenticationService authenticationService;

    public void start() throws Exception {
        server = new Server();

        ServerConnector connector = new ServerConnector(server);
        connector.setPort(8080);
        server.addConnector(connector);

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        ServletHolder servletHolder = new ServletHolder(new CustomGraphQLServlet(true, coordinator, authenticationService));
        context.addServlet(servletHolder, "/graphql/*");
        ServletHolder schema = new ServletHolder(new SchemaGraphQLServlet(coordinator, authenticationService));
        context.addServlet(schema, "/graphql-schema");
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

    public void setCoordinator(Coordinator coordinator) {
        this.coordinator = coordinator;
    }

    public Coordinator getCoordinator() {
        return coordinator;
    }

    public AuthenticationService getAuthenticationService() {
        return authenticationService;
    }

    public void setAuthenticationService(AuthenticationService authenticationService) {
        this.authenticationService = authenticationService;
    }
}
