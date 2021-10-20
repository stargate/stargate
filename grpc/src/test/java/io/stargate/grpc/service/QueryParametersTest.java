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
package io.stargate.grpc.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.StringValue;
import io.stargate.db.Parameters;
import io.stargate.db.Result;
import io.stargate.db.Result.Prepared;
import io.stargate.db.Result.ResultMetadata;
import io.stargate.db.Statement;
import io.stargate.grpc.Utils;
import io.stargate.proto.QueryOuterClass;
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

public class QueryParametersTest extends BaseGrpcServiceTest {
  @ParameterizedTest
  @MethodSource({"queryParameterValues"})
  public void queryParameters(QueryParameters actual, Parameters expected) {
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

    if (actual.hasKeyspace()) {
      when(persistence.decorateKeyspaceName(anyString(), any()))
          .thenReturn("decorated_" + actual.getKeyspace().getValue());
    }

    startServer(persistence);

    StargateBlockingStub stub = makeBlockingStub();

    QueryOuterClass.Response response =
        stub.executeQuery(
            Query.newBuilder().setCql("SELECT * FROM test").setParameters(actual).build());
    assertThat(response.hasResultSet()).isTrue();
  }

  public static Stream<Arguments> queryParameterValues() {
    return Stream.of(
        arguments(
            cqlQueryParameters().build(),
            Parameters.builder()
                .pageSize(GrpcService.DEFAULT_PAGE_SIZE)
                .consistencyLevel(GrpcService.DEFAULT_CONSISTENCY)
                .serialConsistencyLevel(GrpcService.DEFAULT_SERIAL_CONSISTENCY)
                .build()),
        arguments(
            cqlQueryParameters().setKeyspace(StringValue.newBuilder().setValue("abc")).build(),
            Parameters.builder()
                .pageSize(GrpcService.DEFAULT_PAGE_SIZE)
                .consistencyLevel(GrpcService.DEFAULT_CONSISTENCY)
                .serialConsistencyLevel(GrpcService.DEFAULT_SERIAL_CONSISTENCY)
                .defaultKeyspace("decorated_abc")
                .build()),
        arguments(
            cqlQueryParameters()
                .setConsistency(ConsistencyValue.newBuilder().setValue(Consistency.THREE))
                .build(),
            Parameters.builder()
                .pageSize(GrpcService.DEFAULT_PAGE_SIZE)
                .consistencyLevel(ConsistencyLevel.THREE)
                .serialConsistencyLevel(GrpcService.DEFAULT_SERIAL_CONSISTENCY)
                .build()),
        arguments(
            cqlQueryParameters()
                .setSerialConsistency(
                    ConsistencyValue.newBuilder().setValue(Consistency.LOCAL_SERIAL))
                .build(),
            Parameters.builder()
                .pageSize(GrpcService.DEFAULT_PAGE_SIZE)
                .consistencyLevel(GrpcService.DEFAULT_CONSISTENCY)
                .serialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL)
                .build()),
        arguments(
            cqlQueryParameters()
                .setNowInSeconds(Int32Value.newBuilder().setValue(12345).build())
                .build(),
            Parameters.builder()
                .pageSize(GrpcService.DEFAULT_PAGE_SIZE)
                .consistencyLevel(GrpcService.DEFAULT_CONSISTENCY)
                .serialConsistencyLevel(GrpcService.DEFAULT_SERIAL_CONSISTENCY)
                .nowInSeconds(12345)
                .build()),
        arguments(
            cqlQueryParameters()
                .setTimestamp(Int64Value.newBuilder().setValue(1234567890).build())
                .build(),
            Parameters.builder()
                .pageSize(GrpcService.DEFAULT_PAGE_SIZE)
                .consistencyLevel(GrpcService.DEFAULT_CONSISTENCY)
                .serialConsistencyLevel(GrpcService.DEFAULT_SERIAL_CONSISTENCY)
                .defaultTimestamp(1234567890)
                .build()),
        arguments(
            cqlQueryParameters().setTracing(true).build(),
            Parameters.builder()
                .pageSize(GrpcService.DEFAULT_PAGE_SIZE)
                .consistencyLevel(GrpcService.DEFAULT_CONSISTENCY)
                .serialConsistencyLevel(GrpcService.DEFAULT_SERIAL_CONSISTENCY)
                .tracingRequested(true)
                .build()),
        arguments(
            cqlQueryParameters().setTracing(false).build(),
            Parameters.builder()
                .pageSize(GrpcService.DEFAULT_PAGE_SIZE)
                .consistencyLevel(GrpcService.DEFAULT_CONSISTENCY)
                .serialConsistencyLevel(GrpcService.DEFAULT_SERIAL_CONSISTENCY)
                .tracingRequested(false)
                .build()),
        arguments(
            cqlQueryParameters()
                .setPageSize(Int32Value.newBuilder().setValue(99999).build())
                .build(),
            Parameters.builder()
                .consistencyLevel(GrpcService.DEFAULT_CONSISTENCY)
                .serialConsistencyLevel(GrpcService.DEFAULT_SERIAL_CONSISTENCY)
                .pageSize(99999)
                .build()),
        arguments(
            cqlQueryParameters()
                .setPagingState(
                    BytesValue.newBuilder()
                        .setValue(ByteString.copyFrom(new byte[] {'a', 'b', 'c'}))
                        .build())
                .build(),
            Parameters.builder()
                .pageSize(GrpcService.DEFAULT_PAGE_SIZE)
                .consistencyLevel(GrpcService.DEFAULT_CONSISTENCY)
                .serialConsistencyLevel(GrpcService.DEFAULT_SERIAL_CONSISTENCY)
                .pagingState(ByteBuffer.wrap(new byte[] {'a', 'b', 'c'}))
                .build()),
        arguments(
            cqlQueryParameters()
                .setKeyspace(StringValue.newBuilder().setValue("def"))
                .setConsistency(ConsistencyValue.newBuilder().setValue(Consistency.LOCAL_QUORUM))
                .setSerialConsistency(ConsistencyValue.newBuilder().setValue(Consistency.SERIAL))
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
                .defaultKeyspace("decorated_def")
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
