package io.stargate.bridge.service.interceptors;

import io.stargate.auth.AuthenticationService;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.Persistence;
import io.stargate.grpc.service.interceptors.NewConnectionInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BridgeNewConnectionInterceptor extends NewConnectionInterceptor {

  static final Logger logger =
      LoggerFactory.getLogger(BridgeNewConnectionInterceptor.class.getName());

  private final String adminToken;

  public BridgeNewConnectionInterceptor(
      Persistence persistence, AuthenticationService authenticationService, String adminToken) {
    super(persistence, authenticationService);
    this.adminToken = adminToken;
  }

  @Override
  protected Persistence.Connection newConnection(RequestInfo info) throws UnauthorizedException {
    if (adminToken.equals(info.token())) {
      logger.debug("newConnection() received gRPC Bridge call with admin token");
      Persistence.Connection connection = persistence.newConnection();
      connection.setCustomProperties(info.headers());
      return connection;
    } else {
      logger.debug(
          "newConnection() received gRPC Bridge call with non-admin token: {}", info.token());
      return super.newConnection(info);
    }
  }
}
