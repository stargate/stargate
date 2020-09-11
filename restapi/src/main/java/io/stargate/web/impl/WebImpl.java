package io.stargate.web.impl;

import io.stargate.auth.AuthenticationService;
import io.stargate.db.Persistence;

public class WebImpl {

    private Persistence persistence;
    private AuthenticationService authenticationService;

    public Persistence getPersistence() {
        return persistence;
    }

    public void setPersistence(Persistence persistence) {
        this.persistence = persistence;
    }

    public AuthenticationService getAuthenticationService() {
        return authenticationService;
    }

    public void setAuthenticationService(AuthenticationService authenticationService) {
        this.authenticationService = authenticationService;
    }

    public void start() throws Exception {
        Server server = new Server(persistence, this.authenticationService);
        server.run("server", "config.yaml");
    }
}
