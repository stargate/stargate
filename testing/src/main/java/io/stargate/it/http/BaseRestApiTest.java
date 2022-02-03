package io.stargate.it.http;

import io.stargate.it.BaseIntegrationTest;

public class BaseRestApiTest extends BaseIntegrationTest {

  @SuppressWarnings("unused") // referenced in @ApiServiceSpec
  public static void buildParameters(ApiServiceParameters.Builder builder) {
    builder.serviceName("REST API Service");
    builder.servicePort(8088);
    builder.servicePortPropertyName("dw.server.connector.port");
    builder.metricsPort(8088);
    builder.serviceStartedMessage("Started RestServiceServer");
    builder.serviceLibDirProperty("stargate.rest.libdir");
    builder.serviceJarBase("sgv2-rest-service");
    builder.bridgeHostPropertyName("dw.stargate.grpc.host");
    builder.bridgePortPropertyName("dw.stargate.grpc.port");
  }
}
