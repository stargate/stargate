package io.stargate.grpc.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;

import com.google.protobuf.Any;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.db.Persistence;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.QueryOuterClass.Payload;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.QueryParameters;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.QueryOuterClass.Values;
import io.stargate.proto.StargateGrpc;
import io.stargate.proto.StargateGrpc.StargateBlockingStub;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public class BaseServiceTest {
  private static final String SERVER_NAME = "ServiceTests";

  private Server server;

  @BeforeEach
  public void setup() {}

  @AfterEach
  public void cleanUp() {
    try {
      if (server != null) {
        server.shutdown().awaitTermination();
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  protected StargateBlockingStub makeBlockingStub() {
    ManagedChannel channel = InProcessChannelBuilder.forName(SERVER_NAME).usePlaintext().build();
    return StargateGrpc.newBlockingStub(channel);
  }

  protected QueryOuterClass.Result executeQuery(
      StargateBlockingStub stub, String cql, Value... values) {
    return stub.executeQuery(
        Query.newBuilder()
            .setCql(cql)
            .setParameters(cqlQueryParameters(values)).build());
  }

  protected static Payload.Builder cqlPayload(Value... values) {
    return Payload.newBuilder()
        .setType(Payload.Type.TYPE_CQL)
        .setValue(
            Any.pack(
                Values.newBuilder()
                    .addAllValues(Arrays.asList(values))
                    .build()));
  }

  protected static QueryParameters.Builder cqlQueryParameters(Value... values) {
    return QueryParameters.newBuilder().setPayload(cqlPayload(values));
  }

  protected void startServer(Persistence persistence) {
    assertThat(server).isNull();
    server =
        InProcessServerBuilder.forName(SERVER_NAME)
            .directExecutor()
            .intercept(new MockInterceptor())
            .addService(new Service(persistence, mock(Metrics.class)))
            .build();
    try {
      server.start();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
