package io.stargate.bridge.service.interceptors;

import io.stargate.auth.AuthenticationService;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.Persistence;
import io.stargate.grpc.service.interceptors.NewConnectionInterceptor;

public class BridgeNewConnectionInterceptor extends NewConnectionInterceptor {

  private final String adminToken;

  public BridgeNewConnectionInterceptor(
      Persistence persistence, AuthenticationService authenticationService, String adminToken) {
    super(persistence, authenticationService);
    this.adminToken = adminToken;
  }

  @Override
  protected Persistence.Connection newConnection(RequestInfo info) throws UnauthorizedException {
    if (adminToken.equals(info.token())) {
      Persistence.Connection connection = persistence.newConnection();
      connection.setCustomProperties(info.headers());
      return connection;
    } else {
      return super.newConnection(info);
    }
  }
}
