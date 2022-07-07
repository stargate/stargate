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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.stargate.db.Batch;
import io.stargate.db.BatchType;
import io.stargate.db.Parameters;
import io.stargate.db.Result;
import io.stargate.db.Result.Prepared;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.Type;
import io.stargate.grpc.Utils;
import io.stargate.grpc.Values;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.StargateGrpc.StargateBlockingStub;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.apache.cassandra.stargate.exceptions.UnhandledClientException;
import org.assertj.core.util.Arrays;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

public class ExecuteBatchTest extends BaseGrpcServiceTest {

  @BeforeAll
  public static void setup() {
    // To verify that concurrent prepares of batches works
    System.setProperty("stargate.grpc.max_concurrent_prepares_for_batch", "2");
  }

  @Test
  public void simpleBatch() {
    Prepared prepared =
        new Prepared(
            Utils.STATEMENT_ID,
            Utils.RESULT_METADATA_ID,
            Utils.makeResultMetadata(),
            Utils.makePreparedMetadata(Column.create("k", Type.Text), Column.create("v", Type.Int)),
            false,
            false);
    when(connection.prepare(anyString(), any(Parameters.class)))
        .thenReturn(CompletableFuture.completedFuture(prepared));

    when(connection.batch(any(Batch.class), any(Parameters.class), anyLong()))
        .then(
            invocation -> {
              Batch batch = invocation.getArgument(0, Batch.class);

              assertThat(batch.type()).isEqualTo(BatchType.LOGGED);
              assertThat(batch.size()).isEqualTo(3);

              assertStatement(prepared, batch.statements().get(0), Values.of("a"), Values.of(1));
              assertStatement(prepared, batch.statements().get(1), Values.of("b"), Values.of(2));
              assertStatement(prepared, batch.statements().get(2), Values.of("c"), Values.of(3));

              return CompletableFuture.completedFuture(new Result.Void());
            });

    when(persistence.newConnection()).thenReturn(connection);

    startServer(persistence);

    StargateBlockingStub stub = makeBlockingStub();

    QueryOuterClass.Response response =
        stub.executeBatch(
            QueryOuterClass.Batch.newBuilder()
                .addQueries(
                    cqlBatchQuery(
                        "INSERT INTO test (k, v) VALUES (?, ?)", Values.of("a"), Values.of(1)))
                .addQueries(
                    cqlBatchQuery(
                        "INSERT INTO test (k, v) VALUES (?, ?)", Values.of("b"), Values.of(2)))
                .addQueries(
                    cqlBatchQuery(
                        "INSERT INTO test (k, v) VALUES (?, ?)", Values.of("c"), Values.of(3)))
                .build());

    assertThat(response.hasResultSet()).isFalse();
  }

  @ParameterizedTest
  @EnumSource(BatchType.class)
  public void batchTypes(BatchType type) {
    Prepared prepared =
        new Prepared(
            Utils.STATEMENT_ID,
            Utils.RESULT_METADATA_ID,
            Utils.makeResultMetadata(),
            Utils.makePreparedMetadata(),
            false,
            false);
    when(connection.prepare(anyString(), any(Parameters.class)))
        .thenReturn(CompletableFuture.completedFuture(prepared));

    when(connection.batch(any(Batch.class), any(Parameters.class), anyLong()))
        .then(
            invocation -> {
              Batch batch = invocation.getArgument(0, Batch.class);
              assertThat(batch.type()).isEqualTo(type);
              return CompletableFuture.completedFuture(new Result.Void());
            });

    when(persistence.newConnection()).thenReturn(connection);

    startServer(persistence);

    StargateBlockingStub stub = makeBlockingStub();

    QueryOuterClass.Response response =
        stub.executeBatch(
            QueryOuterClass.Batch.newBuilder()
                .setType(QueryOuterClass.Batch.Type.forNumber(type.id))
                .addQueries(cqlBatchQuery("INSERT INTO test (k, v) VALUES ('a', 1)"))
                .build());

    assertThat(response.hasResultSet()).isFalse();
  }

  @Test
  public void noQueries() {
    when(persistence.newConnection()).thenReturn(connection);

    startServer(persistence);

    StargateBlockingStub stub = makeBlockingStub();

    assertThatThrownBy(
            () -> {
              QueryOuterClass.Response response =
                  stub.executeBatch(QueryOuterClass.Batch.newBuilder().build());
              assertThat(response.hasResultSet()).isFalse();
            })
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("No queries in batch");
  }

  @ParameterizedTest
  @MethodSource({"invalidValues"})
  public void invalidValuesTest(Column[] columns, Value[] values, String expectedMessage) {
    Prepared prepared =
        new Prepared(
            Utils.STATEMENT_ID,
            Utils.RESULT_METADATA_ID,
            Utils.makeResultMetadata(),
            Utils.makePreparedMetadata(columns),
            false,
            false);
    when(connection.prepare(anyString(), any(Parameters.class)))
        .thenReturn(CompletableFuture.completedFuture(prepared));

    when(persistence.newConnection()).thenReturn(connection);

    startServer(persistence);

    StargateBlockingStub stub = makeBlockingStub();

    assertThatThrownBy(
            () -> {
              QueryOuterClass.Response response =
                  stub.executeBatch(
                      QueryOuterClass.Batch.newBuilder()
                          .addQueries(cqlBatchQuery("DOES NOT MATTER", values))
                          .build());
              assertThat(response).isNotNull(); // Never going to happen
            })
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining(expectedMessage);
  }

  public static Stream<Arguments> invalidValues() {
    return Stream.of(
        // Invalid arity
        arguments(
            Arrays.array(Column.create("k", Type.Text), Column.create("v", Type.Int)),
            Arrays.array(Values.of("a")),
            "Invalid number of bind values. Expected 2, but received 1"),
        // Invalid type
        arguments(
            Arrays.array(Column.create("k", Type.Text)),
            Arrays.array(Values.of(1)),
            "Invalid argument at position 1"));
  }

  @Test
  public void warnings() {
    Prepared prepared = Utils.makePrepared();

    List<String> expectedWarnings = java.util.Arrays.asList("warning 1", "warning 2");

    when(connection.prepare(anyString(), any(Parameters.class)))
        .thenReturn(CompletableFuture.completedFuture(prepared));

    when(connection.batch(any(Batch.class), any(Parameters.class), anyLong()))
        .thenReturn(
            CompletableFuture.completedFuture(new Result.Void().setWarnings(expectedWarnings)));

    when(persistence.newConnection()).thenReturn(connection);

    startServer(persistence);

    StargateBlockingStub stub = makeBlockingStub();

    QueryOuterClass.Response response =
        stub.executeBatch(
            QueryOuterClass.Batch.newBuilder()
                .addQueries(cqlBatchQuery("INSERT INTO test (k, v) VALUES ('a', 1)"))
                .build());

    assertThat(response.getWarningsList()).containsAll(expectedWarnings);
  }

  @Test
  public void unhandledClientException() {
    Prepared prepared = Utils.makePrepared();

    when(connection.prepare(anyString(), any(Parameters.class)))
        .thenReturn(CompletableFuture.completedFuture(prepared));

    when(connection.batch(any(Batch.class), any(Parameters.class), anyLong()))
        .thenThrow(new UnhandledClientException(""));

    when(persistence.newConnection()).thenReturn(connection);

    startServer(persistence);

    StargateBlockingStub stub = makeBlockingStub();

    assertThatThrownBy(
            () -> {
              QueryOuterClass.Response response =
                  stub.executeBatch(
                      QueryOuterClass.Batch.newBuilder()
                          .addQueries(cqlBatchQuery("INSERT INTO test (k, v) VALUES ('a', 1)"))
                          .build());
            })
        .isInstanceOf(StatusRuntimeException.class)
        .extracting("status")
        .extracting("code")
        .isEqualTo(Status.UNAVAILABLE.getCode());
  }
}
