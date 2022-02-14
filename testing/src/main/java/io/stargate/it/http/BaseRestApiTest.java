package io.stargate.it.http;

import io.stargate.it.BaseIntegrationTest;

/** Base class for all REST API tests. */
public class BaseRestApiTest extends BaseIntegrationTest {

  /**
   * Used to define common parameters for configuring REST API service instances. If you need to
   * add/override parameters in a subclass, provide a different parametersCustomizer, but make sure
   * that it invokes this method as well.
   *
   * @see ApiServiceSpec#parametersCustomizer()
   */
  public static void buildApiServiceParameters(ApiServiceParameters.Builder builder) {
    builder.serviceName("rest-api");
    builder.servicePort(8082);
    builder.servicePortPropertyName("dw.server.connector.port");
    builder.metricsPort(8082);
    builder.serviceStartedMessage("Started RestServiceServer");
    builder.serviceLibDirProperty("stargate.rest.libdir");
    builder.serviceJarBase("sgv2-rest-service");
    builder.bridgeHostPropertyName("dw.stargate.bridge.host");
    builder.bridgePortPropertyName("dw.stargate.bridge.port");
  }
}
