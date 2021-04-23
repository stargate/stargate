package io.stargate.grpc.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.StringValue;
import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.Persistence.Connection;
import io.stargate.db.Result;
import io.stargate.db.Result.Prepared;
import io.stargate.db.Result.ResultMetadata;
import io.stargate.db.Statement;
import io.stargate.grpc.Utils;
import io.stargate.proto.QueryOuterClass.Consistency;
import io.stargate.proto.QueryOuterClass.ConsistencyValue;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.QueryParameters;
import io.stargate.proto.StargateGrpc.StargateBlockingStub;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class QueryParametersTest extends BaseServiceTest {
  @ParameterizedTest
  @MethodSource({"queryParameterValues"})
  public void queryParameters(QueryParameters actual, Parameters expected) {
    Persistence persistence = mock(Persistence.class);
    Connection connection = mock(Connection.class);

    ResultMetadata resultMetadata = Utils.makeResultMetadata();
    Prepared prepared = Utils.makePrepared();

    when(connection.prepare(anyString(), any(Parameters.class)))
        .thenReturn(CompletableFuture.completedFuture(prepared));

    when(connection.execute(any(Statement.class), any(Parameters.class), anyLong()))
        .then(
            invocation -> {
              Parameters parameters = invocation.getArgument(1, Parameters.class);
              assertThat(parameters).isEqualTo(expected);
              return CompletableFuture.completedFuture(
                  new Result.Rows(Collections.emptyList(), resultMetadata));
            });

    when(persistence.newConnection()).thenReturn(connection);

    startServer(persistence);

    StargateBlockingStub stub = makeBlockingStub();

    stub.executeQuery(
        Query.newBuilder().setCql("SELECT * FROM test").setParameters(actual).build());
  }

  public static Stream<Arguments> queryParameterValues() {
    return Stream.of(
        arguments(cqlQueryParameters().build(), Parameters.builder().build()),
        arguments(
            cqlQueryParameters().setKeyspace(StringValue.newBuilder().setValue("abc")).build(),
            Parameters.builder().defaultKeyspace("abc").build()),
        arguments(
            cqlQueryParameters()
                .setConsistency(
                    ConsistencyValue.newBuilder().setValue(Consistency.CONSISTENCY_THREE))
                .build(),
            Parameters.builder().consistencyLevel(ConsistencyLevel.THREE).build()),
        arguments(
            cqlQueryParameters()
                .setSerialConsistency(
                    ConsistencyValue.newBuilder().setValue(Consistency.CONSISTENCY_LOCAL_SERIAL))
                .build(),
            Parameters.builder().serialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL).build()),
        arguments(
            cqlQueryParameters()
                .setNowInSeconds(Int32Value.newBuilder().setValue(12345).build())
                .build(),
            Parameters.builder().nowInSeconds(12345).build()),
        arguments(
            cqlQueryParameters()
                .setTimestamp(Int64Value.newBuilder().setValue(1234567890).build())
                .build(),
            Parameters.builder().defaultTimestamp(1234567890).build()),
        arguments(
            cqlQueryParameters().setTracing(true).build(),
            Parameters.builder().tracingRequested(true).build()),
        arguments(
            cqlQueryParameters().setTracing(false).build(),
            Parameters.builder().tracingRequested(false).build()),
        arguments(
            cqlQueryParameters()
                .setPageSize(Int32Value.newBuilder().setValue(99999).build())
                .build(),
            Parameters.builder().pageSize(99999).build()),
        arguments(
            cqlQueryParameters()
                .setPagingState(
                    BytesValue.newBuilder()
                        .setValue(ByteString.copyFrom(new byte[] {'a', 'b', 'c'}))
                        .build())
                .build(),
            Parameters.builder().pagingState(ByteBuffer.wrap(new byte[] {'a', 'b', 'c'})).build()),
        arguments(
            cqlQueryParameters()
                .setKeyspace(StringValue.newBuilder().setValue("def"))
                .setConsistency(
                    ConsistencyValue.newBuilder().setValue(Consistency.CONSISTENCY_LOCAL_QUORUM))
                .setSerialConsistency(
                    ConsistencyValue.newBuilder().setValue(Consistency.CONSISTENCY_SERIAL))
                .setNowInSeconds(Int32Value.newBuilder().setValue(54321).build())
                .setTimestamp(Int64Value.newBuilder().setValue(1234567890).build())
                .setTracing(true)
                .setPageSize(Int32Value.newBuilder().setValue(1000).build())
                .setPagingState(
                    BytesValue.newBuilder()
                        .setValue(ByteString.copyFrom(new byte[] {'d', 'e', 'f'}))
                        .build())
                .build(),
            Parameters.builder()
                .defaultKeyspace("def")
                .consistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
                .serialConsistencyLevel(ConsistencyLevel.SERIAL)
                .nowInSeconds(54321)
                .defaultTimestamp(1234567890)
                .tracingRequested(true)
                .pageSize(1000)
                .pagingState(ByteBuffer.wrap(new byte[] {'d', 'e', 'f'}))
                .build()));
  }
}
