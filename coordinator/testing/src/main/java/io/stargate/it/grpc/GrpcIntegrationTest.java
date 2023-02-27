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
package io.stargate.it.grpc;

import static io.stargate.proto.ReactorStargateGrpc.*;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.StringValue;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.stargate.auth.model.AuthTokenResponse;
import io.stargate.grpc.StargateBearerToken;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.http.RestUtils;
import io.stargate.it.http.models.Credentials;
import io.stargate.it.storage.IfBundleAvailable;
import io.stargate.it.storage.StargateConnectionInfo;
import io.stargate.proto.QueryOuterClass.BatchParameters;
import io.stargate.proto.QueryOuterClass.BatchQuery;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.QueryParameters;
import io.stargate.proto.QueryOuterClass.Row;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.QueryOuterClass.Values;
import io.stargate.proto.ReactorStargateGrpc;
import io.stargate.proto.StargateGrpc;
import io.stargate.proto.StargateGrpc.StargateBlockingStub;
import io.stargate.proto.StargateGrpc.StargateStub;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

@IfBundleAvailable(bundleName = "grpc")
public class GrpcIntegrationTest extends BaseIntegrationTest {
  private static final ObjectMapper objectMapper = new ObjectMapper();

  protected static ManagedChannel managedChannel;
  protected static StargateBlockingStub stub;
  protected static StargateStub asyncStub;
  private static ReactorStargateStub reactorStub;
  protected static String authToken;

  @BeforeAll
  public static void setup(StargateConnectionInfo cluster) throws IOException {
    String seedAddress = cluster.seedAddress();

    managedChannel =
        ManagedChannelBuilder.forAddress(seedAddress, 8090)
            .usePlaintext()
            .userAgent("stargate-int-tests/v1")
            .build();
    stub = StargateGrpc.newBlockingStub(managedChannel);
    asyncStub = StargateGrpc.newStub(managedChannel);
    reactorStub = ReactorStargateGrpc.newReactorStub(managedChannel);

    authToken =
        generateAuthToken(String.format("http://%s:8081", seedAddress), "cassandra", "cassandra");
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

  protected StargateBlockingStub stubWithCallCredentials(String token) {
    return stub.withCallCredentials(new StargateBearerToken(token));
  }

  protected StargateStub asyncStubWithCallCredentials(String token) {
    return asyncStub.withCallCredentials(new StargateBearerToken(token));
  }

  protected StargateBlockingStub stubWithCallCredentials() {
    return stubWithCallCredentials(authToken);
  }

  protected StargateStub asyncStubWithCallCredentials() {
    return asyncStubWithCallCredentials(authToken);
  }

  protected ReactorStargateStub reactiveStubWithCallCredentials() {
    return reactorStub.withCallCredentials(new StargateBearerToken(authToken));
  }

  protected QueryParameters.Builder queryParameters(
      CqlIdentifier keyspace, boolean tracingEnabled) {
    return QueryParameters.newBuilder()
        .setKeyspace(StringValue.of(keyspace.toString()))
        .setTracing(tracingEnabled);
  }

  protected QueryParameters.Builder queryParameters(CqlIdentifier keyspace) {
    return queryParameters(keyspace, false);
  }

  protected BatchParameters.Builder batchParameters(CqlIdentifier keyspace) {
    return batchParameters(keyspace, false);
  }

  protected BatchParameters.Builder batchParameters(
      CqlIdentifier keyspace, boolean tracingEnabled) {
    return BatchParameters.newBuilder()
        .setKeyspace(StringValue.of(keyspace.toString()))
        .setTracing(tracingEnabled);
  }

  protected static BatchQuery cqlBatchQuery(String cql, Value... values) {
    return BatchQuery.newBuilder().setCql(cql).setValues(valuesOf(values)).build();
  }

  protected static Query cqlQuery(String cql, Value... values) {
    return Query.newBuilder().setCql(cql).setValues(valuesOf(values)).build();
  }

  protected static Query cqlQuery(String cql, QueryParameters.Builder parameters, Value... values) {
    return Query.newBuilder()
        .setCql(cql)
        .setParameters(parameters)
        .setValues(valuesOf(values))
        .build();
  }

  protected static Values valuesOf(Value... values) {
    return Values.newBuilder().addAllValues(Arrays.asList(values)).build();
  }

  protected static Row rowOf(Value... values) {
    return Row.newBuilder().addAllValues(Arrays.asList(values)).build();
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
