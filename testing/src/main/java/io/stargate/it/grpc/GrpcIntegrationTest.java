package io.stargate.it.grpc;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.stargate.auth.model.AuthTokenResponse;
import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.http.RestUtils;
import io.stargate.it.http.models.Credentials;
import io.stargate.it.storage.IfBundleAvailable;
import io.stargate.it.storage.StargateConnectionInfo;
import io.stargate.proto.StargateGrpc;
import java.io.IOException;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.BeforeEach;

@IfBundleAvailable(bundleName = "grpc")
public class GrpcIntegrationTest extends BaseOsgiIntegrationTest {
  private static final ObjectMapper objectMapper = new ObjectMapper();

  protected StargateGrpc.StargateBlockingStub stub;
  protected String authToken;

  @BeforeEach
  public void setup(StargateConnectionInfo cluster) throws IOException {
    String seedAddress = cluster.seedAddress();

    ManagedChannel channel =
        ManagedChannelBuilder.forAddress(seedAddress, 8090).usePlaintext().build();
    stub = StargateGrpc.newBlockingStub(channel);

    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    String body =
        RestUtils.post(
            "",
            String.format("http://%s:8081/v1/auth/token/generate", seedAddress),
            objectMapper.writeValueAsString(new Credentials("cassandra", "cassandra")),
            HttpStatus.SC_CREATED);

    AuthTokenResponse authTokenResponse = objectMapper.readValue(body, AuthTokenResponse.class);
    authToken = authTokenResponse.getAuthToken();
    assertThat(authToken).isNotNull();
  }
}
