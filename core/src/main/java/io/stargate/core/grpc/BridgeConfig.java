package io.stargate.core.grpc;

public class BridgeConfig {

  /**
   * The token that can be used by Stargate services to query the gRPC bridge when no user token is
   * available (e.g. background queries).
   */
  public static final String ADMIN_TOKEN;

  static {
    String propertyName = "stargate.bridge.admin_token";
    ADMIN_TOKEN = System.getProperty(propertyName);
    if (ADMIN_TOKEN == null || ADMIN_TOKEN.isEmpty()) {
      throw new IllegalStateException(String.format("'%s' must be provided", propertyName));
    }
  }
}
