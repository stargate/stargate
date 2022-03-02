/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.it.bridge;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.stargate.auth.model.AuthTokenResponse;
import io.stargate.grpc.StargateBearerToken;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.http.RestUtils;
import io.stargate.it.http.models.Credentials;
import io.stargate.it.storage.IfBundleAvailable;
import io.stargate.it.storage.StargateConnectionInfo;
import io.stargate.proto.StargateBridgeGrpc;
import io.stargate.proto.StargateBridgeGrpc.StargateBridgeBlockingStub;
import io.stargate.proto.StargateBridgeGrpc.StargateBridgeStub;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

@IfBundleAvailable(bundleName = "bridge")
public class BridgeIntegrationTest extends BaseIntegrationTest {
  private static final ObjectMapper objectMapper = new ObjectMapper();

  protected static ManagedChannel managedChannel;
  protected static StargateBridgeBlockingStub stub;
  protected static StargateBridgeStub asyncStub;
  protected static String authToken;

  @BeforeAll
  public static void setup(StargateConnectionInfo cluster) throws IOException {
    String seedAddress = cluster.seedAddress();

    managedChannel = ManagedChannelBuilder.forAddress(seedAddress, 8091).usePlaintext().build();
    stub = StargateBridgeGrpc.newBlockingStub(managedChannel);
    asyncStub = StargateBridgeGrpc.newStub(managedChannel);

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

  @AfterAll
  public static void cleanUp() {
    managedChannel.shutdown();
    try {
      if (!managedChannel.awaitTermination(3, TimeUnit.SECONDS)) {
        managedChannel.shutdownNow();
        if (!managedChannel.awaitTermination(5, TimeUnit.SECONDS)) {
          throw new RuntimeException("ManagedChannel failed to terminate, aborting..");
        }
      }
    } catch (InterruptedException ie) {
      managedChannel.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  protected StargateBridgeBlockingStub stubWithCallCredentials() {
    return stub.withCallCredentials(new StargateBearerToken(authToken));
  }

  protected StargateBridgeBlockingStub stubWithCallCredentials(String token) {
    return stub.withCallCredentials(new StargateBearerToken(token));
  }

  protected static String generateAuthToken(String authUrlBase, String username, String password)
      throws IOException {
    String body =
        RestUtils.post(
            "",
            String.format("%s/v1/auth/token/generate", authUrlBase),
            objectMapper.writeValueAsString(new Credentials(username, password)),
            HttpStatus.SC_CREATED);

    AuthTokenResponse authTokenResponse = objectMapper.readValue(body, AuthTokenResponse.class);
    String authToken = authTokenResponse.getAuthToken();
    assertThat(authToken).isNotNull();

    return authToken;
  }
}
