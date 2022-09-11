package io.stargate.it.http.dynamo;

import static org.assertj.core.api.Assertions.assertThat;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.auth.model.AuthTokenResponse;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.http.ApiServiceConnectionInfo;
import io.stargate.it.http.ApiServiceParameters;
import io.stargate.it.http.RestUtils;
import io.stargate.it.http.models.Credentials;
import io.stargate.it.storage.StargateConnectionInfo;
import java.io.IOException;
import java.util.Properties;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

public class BaseDynamoApiTest extends BaseIntegrationTest {
  protected AmazonDynamoDB awsClient = DynamoDBEmbedded.create().amazonDynamoDB();
  protected AmazonDynamoDB proxyClient;
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private String dynamoUrlBase;
  private String authToken;

  public static void buildApiServiceParameters(ApiServiceParameters.Builder builder) {
    builder.serviceName("dynamo-api");
    builder.servicePort(8082);
    builder.servicePortPropertyName("dw.server.connector.port");
    builder.metricsPort(8082);
    builder.serviceStartedMessage("Started DynamoServiceServer");
    builder.serviceLibDirProperty("stargate.dynamo.libdir");
    builder.serviceJarBase("sgv2-dynamoapi");
    builder.bridgeHostPropertyName("dw.stargate.bridge.host");
    builder.bridgePortPropertyName("dw.stargate.bridge.port");
  }

  @BeforeEach
  public void setup(
      TestInfo testInfo, StargateConnectionInfo cluster, ApiServiceConnectionInfo dynamoApi)
      throws IOException {
    dynamoUrlBase = "http://" + dynamoApi.host() + ":" + dynamoApi.port();
    String authUrlBase =
        "http://" + cluster.seedAddress() + ":8081"; // TODO: make auth port configurable

    String body =
        RestUtils.post(
            "",
            String.format("%s/v1/auth/token/generate", authUrlBase),
            objectMapper.writeValueAsString(new Credentials("cassandra", "cassandra")),
            HttpStatus.SC_CREATED);

    AuthTokenResponse authTokenResponse = objectMapper.readValue(body, AuthTokenResponse.class);
    authToken = authTokenResponse.getAuthToken();
    assertThat(authToken).isNotNull();

    createTestKeyspace("dynamodb");

    Properties props = System.getProperties();
    props.setProperty("aws.accessKeyId", authToken);
    props.setProperty("aws.secretKey", "fake-secret-key");
    AwsClientBuilder.EndpointConfiguration endpointConfiguration =
        new AwsClientBuilder.EndpointConfiguration(dynamoUrlBase, "fake-region");
    proxyClient =
        AmazonDynamoDBClientBuilder.standard()
            .withEndpointConfiguration(endpointConfiguration)
            .build();
  }

  private void createTestKeyspace(String keyspaceName) throws IOException {
    RestUtils.get(
        "Authorization",
        authToken,
        String.format("%s/keyspace/create", dynamoUrlBase),
        HttpStatus.SC_CREATED);
  }
}
