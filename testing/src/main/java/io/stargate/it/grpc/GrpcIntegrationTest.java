package io.stargate.it.grpc;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Any;
import com.google.protobuf.StringValue;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.stargate.auth.model.AuthTokenResponse;
import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.http.RestUtils;
import io.stargate.it.http.models.Credentials;
import io.stargate.it.storage.IfBundleAvailable;
import io.stargate.it.storage.StargateConnectionInfo;
import io.stargate.proto.QueryOuterClass.BatchParameters;
import io.stargate.proto.QueryOuterClass.BatchQuery;
import io.stargate.proto.QueryOuterClass.Payload;
import io.stargate.proto.QueryOuterClass.Payload.Type;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.QueryParameters;
import io.stargate.proto.QueryOuterClass.Row;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.QueryOuterClass.Values;
import io.stargate.proto.StargateGrpc;
import io.stargate.proto.StargateGrpc.StargateBlockingStub;
import java.io.IOException;
import java.util.Arrays;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.BeforeEach;

@IfBundleAvailable(bundleName = "grpc")
public class GrpcIntegrationTest extends BaseOsgiIntegrationTest {
  private static final ObjectMapper objectMapper = new ObjectMapper();

  protected StargateBlockingStub stub;
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

  protected StargateBlockingStub stubWithCallCredentials(String token) {
    return stub.withCallCredentials(new StargateBearerToken(token));
  }

  protected StargateBlockingStub stubWithCallCredentials() {
    return stubWithCallCredentials(authToken);
  }

  protected QueryParameters.Builder queryParameters(CqlIdentifier keyspace) {
    return QueryParameters.newBuilder().setKeyspace(StringValue.of(keyspace.toString()));
  }

  protected BatchParameters.Builder batchParameters(CqlIdentifier keyspace) {
    return BatchParameters.newBuilder().setKeyspace(StringValue.of(keyspace.toString()));
  }

  protected static BatchQuery cqlBatchQuery(String cql, Value... values) {
    return BatchQuery.newBuilder()
        .setCql(cql)
        .setValues(
            Payload.newBuilder().setType(Type.CQL).setData(Any.pack(cqlValues(values))).build())
        .build();
  }

  protected static Query cqlQuery(String cql, QueryParameters.Builder parameters, Value... values) {
    return Query.newBuilder()
        .setCql(cql)
        .setParameters(parameters)
        .setValues(
            Payload.newBuilder().setType(Type.CQL).setData(Any.pack(cqlValues(values))).build())
        .build();
  }

  protected static Values cqlValues(Value... values) {
    return Values.newBuilder().addAllValues(Arrays.asList(values)).build();
  }

  protected static Row cqlRow(Value... values) {
    return Row.newBuilder().addAllValues(Arrays.asList(values)).build();
  }
}
