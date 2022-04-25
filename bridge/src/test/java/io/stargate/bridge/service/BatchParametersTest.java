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
package io.stargate.bridge.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.StringValue;
import io.stargate.bridge.Utils;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.QueryOuterClass.BatchParameters;
import io.stargate.bridge.proto.QueryOuterClass.Consistency;
import io.stargate.bridge.proto.QueryOuterClass.ConsistencyValue;
import io.stargate.bridge.proto.StargateBridgeGrpc.StargateBridgeBlockingStub;
import io.stargate.db.Batch;
import io.stargate.db.Parameters;
import io.stargate.db.Result;
import io.stargate.db.Result.Prepared;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class BatchParametersTest extends BaseBridgeServiceTest {
  @ParameterizedTest
  @MethodSource({"batchParameterValues"})
  public void batchParameters(BatchParameters actual, Parameters expected) {
    Prepared prepared = Utils.makePrepared();

    when(connection.prepare(anyString(), any(Parameters.class)))
        .thenReturn(CompletableFuture.completedFuture(prepared));

    when(connection.batch(any(Batch.class), any(Parameters.class), anyLong()))
        .then(
            invocation -> {
              Parameters parameters = invocation.getArgument(1, Parameters.class);
              assertThat(parameters).isEqualTo(expected);
              return CompletableFuture.completedFuture(new Result.Void());
            });

    when(persistence.newConnection()).thenReturn(connection);

    if (actual.hasKeyspace()) {
      when(persistence.decorateKeyspaceName(anyString(), any()))
          .thenReturn("decorated_" + actual.getKeyspace().getValue());
    }

    startServer(persistence);

    StargateBridgeBlockingStub stub = makeBlockingStub();

    QueryOuterClass.Response response =
        stub.executeBatch(
            QueryOuterClass.Batch.newBuilder()
                .addQueries(cqlBatchQuery("DOES NOT MATTER"))
                .setParameters(actual)
                .build());
    assertThat(response.hasResultSet()).isFalse();
  }

  public static Stream<Arguments> batchParameterValues() {
    return Stream.of(
        arguments(
            batchParameters().build(),
            Parameters.builder()
                .consistencyLevel(BridgeService.DEFAULT_CONSISTENCY)
                .serialConsistencyLevel(BridgeService.DEFAULT_SERIAL_CONSISTENCY)
                .build()),
        arguments(
            batchParameters().setKeyspace(StringValue.newBuilder().setValue("abc")).build(),
            Parameters.builder()
                .consistencyLevel(BridgeService.DEFAULT_CONSISTENCY)
                .serialConsistencyLevel(BridgeService.DEFAULT_SERIAL_CONSISTENCY)
                .defaultKeyspace("decorated_abc")
                .build()),
        arguments(
            batchParameters()
                .setConsistency(ConsistencyValue.newBuilder().setValue(Consistency.THREE))
                .build(),
            Parameters.builder()
                .consistencyLevel(ConsistencyLevel.THREE)
                .serialConsistencyLevel(BridgeService.DEFAULT_SERIAL_CONSISTENCY)
                .build()),
        arguments(
            batchParameters()
                .setSerialConsistency(
                    ConsistencyValue.newBuilder().setValue(Consistency.LOCAL_SERIAL))
                .build(),
            Parameters.builder()
                .consistencyLevel(BridgeService.DEFAULT_CONSISTENCY)
                .serialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL)
                .build()),
        arguments(
            batchParameters()
                .setNowInSeconds(Int32Value.newBuilder().setValue(12345).build())
                .build(),
            Parameters.builder()
                .consistencyLevel(BridgeService.DEFAULT_CONSISTENCY)
                .serialConsistencyLevel(BridgeService.DEFAULT_SERIAL_CONSISTENCY)
                .nowInSeconds(12345)
                .build()),
        arguments(
            batchParameters()
                .setTimestamp(Int64Value.newBuilder().setValue(1234567890).build())
                .build(),
            Parameters.builder()
                .consistencyLevel(BridgeService.DEFAULT_CONSISTENCY)
                .serialConsistencyLevel(BridgeService.DEFAULT_SERIAL_CONSISTENCY)
                .defaultTimestamp(1234567890)
                .build()),
        arguments(
            batchParameters().setTracing(true).build(),
            Parameters.builder()
                .consistencyLevel(BridgeService.DEFAULT_CONSISTENCY)
                .serialConsistencyLevel(BridgeService.DEFAULT_SERIAL_CONSISTENCY)
                .tracingRequested(true)
                .build()),
        arguments(
            batchParameters().setTracing(false).build(),
            Parameters.builder()
                .consistencyLevel(BridgeService.DEFAULT_CONSISTENCY)
                .serialConsistencyLevel(BridgeService.DEFAULT_SERIAL_CONSISTENCY)
                .tracingRequested(false)
                .build()),
        arguments(
            batchParameters()
                .setKeyspace(StringValue.newBuilder().setValue("def"))
                .setConsistency(ConsistencyValue.newBuilder().setValue(Consistency.LOCAL_QUORUM))
                .setSerialConsistency(ConsistencyValue.newBuilder().setValue(Consistency.SERIAL))
                .setNowInSeconds(Int32Value.newBuilder().setValue(54321).build())
                .setTimestamp(Int64Value.newBuilder().setValue(1234567890).build())
                .setTracing(true)
                .build(),
            Parameters.builder()
                .defaultKeyspace("decorated_def")
                .consistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
                .serialConsistencyLevel(ConsistencyLevel.SERIAL)
                .nowInSeconds(54321)
                .defaultTimestamp(1234567890)
                .tracingRequested(true)
                .build()));
  }

  private static BatchParameters.Builder batchParameters() {
    return BatchParameters.newBuilder();
  }
}
