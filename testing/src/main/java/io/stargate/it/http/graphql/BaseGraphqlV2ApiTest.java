package io.stargate.it.http.graphql;

import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.http.ApiServiceExtension;
import io.stargate.it.http.ApiServiceParameters;
import io.stargate.it.http.ApiServiceSpec;
import org.junit.jupiter.api.extension.ExtendWith;

/** Base class for integration tests that run against the Stargate v2 GraphQL API. */
@ExtendWith(ApiServiceExtension.class)
@ApiServiceSpec(parametersCustomizer = "buildGraphqlServiceParameters")
public abstract class BaseGraphqlV2ApiTest extends BaseIntegrationTest {

  public static void buildGraphqlServiceParameters(ApiServiceParameters.Builder builder) {
    builder.serviceName("graphql-api");
    builder.servicePort(8082);
    builder.servicePortPropertyName("dw.server.connector.port");
    builder.metricsPort(8082);
    builder.serviceStartedMessage("Started GraphqlServiceServer");
    builder.serviceLibDirProperty("stargate.graphql.libdir");
    builder.serviceJarBase("sgv2-graphqlapi");
    builder.addServiceArguments("--timeout-seconds", "10");
    builder.bridgeHostPropertyName("dw.stargate.bridge.host");
    builder.bridgePortPropertyName("dw.stargate.bridge.port");
  }
}
