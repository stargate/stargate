package io.stargate.web.impl;

import io.stargate.auth.AuthenticationService;
import io.stargate.coordinator.Coordinator;

public class WebImpl {

    private Coordinator coordinator;
    private AuthenticationService authenticationService;

    public Coordinator getCoordinator() {
        return coordinator;
    }

    public void setCoordinator(Coordinator coordinator) {
        this.coordinator = coordinator;
    }

    public AuthenticationService getAuthenticationService() {
        return authenticationService;
    }

    public void setAuthenticationService(AuthenticationService authenticationService) {
        this.authenticationService = authenticationService;
    }

    public void start() throws Exception {
        Server server = new Server(coordinator.getPersistence(), this.authenticationService);
        server.run("server", "config.yaml");
    }
}
