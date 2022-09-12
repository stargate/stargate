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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.StatusRuntimeException;
import io.stargate.bridge.Utils;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.QueryOuterClass.ResultSet;
import io.stargate.bridge.proto.StargateBridgeGrpc.StargateBridgeBlockingStub;
import io.stargate.db.Batch;
import io.stargate.db.BatchType;
import io.stargate.db.BoundStatement;
import io.stargate.db.Parameters;
import io.stargate.db.Result;
import io.stargate.db.Result.Prepared;
import io.stargate.db.Result.ResultMetadata;
import io.stargate.db.Statement;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.Type;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.apache.cassandra.stargate.db.WriteType;
import org.apache.cassandra.stargate.exceptions.ReadTimeoutException;
import org.apache.cassandra.stargate.exceptions.WriteTimeoutException;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

public class RetryTest extends BaseBridgeServiceTest {

  @Test
  public void shouldNotRetryOnReadTimeoutWhenDataPresent() {
    final String query = "SELECT release_version FROM system.local WHERE key = ?";
    final String releaseVersion = "4.0.0";

    ResultMetadata resultMetadata =
        Utils.makeResultMetadata(Column.create("release_version", Type.Text));
    Prepared prepared =
        new Prepared(
            Utils.STATEMENT_ID,
            Utils.RESULT_METADATA_ID,
            resultMetadata,
            Utils.makePreparedMetadata(Column.create("key", Type.Text)),
            true,
            false);
    when(connection.prepare(eq(query), any(Parameters.class)))
        .thenReturn(CompletableFuture.completedFuture(prepared));

    when(connection.execute(any(Statement.class), any(Parameters.class), anyLong()))
        .thenThrow(new ReadTimeoutException(ConsistencyLevel.QUORUM, 1, 3, true))
        .then(correctResponse(releaseVersion, resultMetadata, prepared));

    when(persistence.newConnection()).thenReturn(connection);

    startServer(persistence);

    StargateBridgeBlockingStub stub = makeBlockingStub();

    assertThatThrownBy(() -> executeQuery(stub, query, Values.of("local")))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("Operation timed out - received only 1 responses");
  }

  @Test
  public void shouldRetryBatchRequestOnReadTimeout() {
    Prepared prepared =
        new Prepared(
            Utils.STATEMENT_ID,
            Utils.RESULT_METADATA_ID,
            Utils.makeResultMetadata(),
            Utils.makePreparedMetadata(Column.create("k", Type.Text), Column.create("v", Type.Int)),
            true,
            false);
    when(connection.prepare(anyString(), any(Parameters.class)))
        .thenReturn(CompletableFuture.completedFuture(prepared));

    when(connection.batch(any(Batch.class), any(Parameters.class), anyLong()))
        .thenThrow(new ReadTimeoutException(ConsistencyLevel.QUORUM, 3, 3, false))
        .then(correctBatchResponse(prepared));

    when(persistence.newConnection()).thenReturn(connection);

    startServer(persistence);

    StargateBridgeBlockingStub stub = makeBlockingStub();

    QueryOuterClass.Response response = stub.executeBatch(createBatch());

    assertThat(response.hasResultSet()).isFalse();
  }

  @Test
  public void shouldRetryBatchRequestOnWriteTimeout() {
    Prepared prepared =
        new Prepared(
            Utils.STATEMENT_ID,
            Utils.RESULT_METADATA_ID,
            Utils.makeResultMetadata(),
            Utils.makePreparedMetadata(Column.create("k", Type.Text), Column.create("v", Type.Int)),
            true,
            false);
    when(connection.prepare(anyString(), any(Parameters.class)))
        .thenReturn(CompletableFuture.completedFuture(prepared));

    when(connection.batch(any(Batch.class), any(Parameters.class), anyLong()))
        .thenThrow(new WriteTimeoutException(WriteType.BATCH_LOG, ConsistencyLevel.QUORUM, 3, 3))
        .then(correctBatchResponse(prepared));

    when(persistence.newConnection()).thenReturn(connection);

    startServer(persistence);

    StargateBridgeBlockingStub stub = makeBlockingStub();

    QueryOuterClass.Response response = stub.executeBatch(createBatch());

    assertThat(response.hasResultSet()).isFalse();
  }

  @Test
  public void shouldNotRetryOnReadTimeoutWhenLessThanBlockForReceived() {
    final String query = "SELECT release_version FROM system.local WHERE key = ?";
    final String releaseVersion = "4.0.0";

    ResultMetadata resultMetadata =
        Utils.makeResultMetadata(Column.create("release_version", Type.Text));
    Prepared prepared =
        new Prepared(
            Utils.STATEMENT_ID,
            Utils.RESULT_METADATA_ID,
            resultMetadata,
            Utils.makePreparedMetadata(Column.create("key", Type.Text)),
            true,
            false);
    when(connection.prepare(eq(query), any(Parameters.class)))
        .thenReturn(CompletableFuture.completedFuture(prepared));

    when(connection.execute(any(Statement.class), any(Parameters.class), anyLong()))
        .thenThrow(new ReadTimeoutException(ConsistencyLevel.QUORUM, 2, 3, false))
        .then(correctResponse(releaseVersion, resultMetadata, prepared));

    when(persistence.newConnection()).thenReturn(connection);

    startServer(persistence);

    StargateBridgeBlockingStub stub = makeBlockingStub();

    assertThatThrownBy(() -> executeQuery(stub, query, Values.of("local")))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("Operation timed out - received only 2 responses");
  }

  @Test
  public void shouldRetryOnReadTimeoutWhenEnoughResponsesAndDataNotPresent()
      throws InvalidProtocolBufferException {
    final String query = "SELECT release_version FROM system.local WHERE key = ?";
    final String releaseVersion = "4.0.0";

    ResultMetadata resultMetadata =
        Utils.makeResultMetadata(Column.create("release_version", Type.Text));
    Prepared prepared =
        new Prepared(
            Utils.STATEMENT_ID,
            Utils.RESULT_METADATA_ID,
            resultMetadata,
            Utils.makePreparedMetadata(Column.create("key", Type.Text)),
            true,
            false);
    when(connection.prepare(eq(query), any(Parameters.class)))
        .thenReturn(CompletableFuture.completedFuture(prepared));

    when(connection.execute(any(Statement.class), any(Parameters.class), anyLong()))
        .thenThrow(new ReadTimeoutException(ConsistencyLevel.QUORUM, 3, 3, false))
        .then(correctResponse(releaseVersion, resultMetadata, prepared));

    when(persistence.newConnection()).thenReturn(connection);

    startServer(persistence);

    StargateBridgeBlockingStub stub = makeBlockingStub();

    QueryOuterClass.Response response = executeQuery(stub, query, Values.of("local"));

    assertThat(response.hasResultSet()).isTrue();
    validateResponse(releaseVersion, response);
  }

  @Test
  public void shouldRetryOnWriteTimeoutIfWriteTypeBatchLog() throws InvalidProtocolBufferException {
    final String query = "SELECT release_version FROM system.local WHERE key = ?";
    final String releaseVersion = "4.0.0";

    ResultMetadata resultMetadata =
        Utils.makeResultMetadata(Column.create("release_version", Type.Text));
    Prepared prepared =
        new Prepared(
            Utils.STATEMENT_ID,
            Utils.RESULT_METADATA_ID,
            resultMetadata,
            Utils.makePreparedMetadata(Column.create("key", Type.Text)),
            true,
            false);
    when(connection.prepare(eq(query), any(Parameters.class)))
        .thenReturn(CompletableFuture.completedFuture(prepared));

    when(connection.execute(any(Statement.class), any(Parameters.class), anyLong()))
        .thenThrow(new WriteTimeoutException(WriteType.BATCH_LOG, ConsistencyLevel.QUORUM, 1, 1))
        .then(correctResponse(releaseVersion, resultMetadata, prepared));

    when(persistence.newConnection()).thenReturn(connection);

    startServer(persistence);

    StargateBridgeBlockingStub stub = makeBlockingStub();

    QueryOuterClass.Response response = executeQuery(stub, query, Values.of("local"));

    assertThat(response.hasResultSet()).isTrue();
    validateResponse(releaseVersion, response);
  }

  @Test
  public void shouldNotRetryOnWriteTimeoutIfWriteTypeNonBatchLog() {
    final String query = "SELECT release_version FROM system.local WHERE key = ?";
    final String releaseVersion = "4.0.0";

    ResultMetadata resultMetadata =
        Utils.makeResultMetadata(Column.create("release_version", Type.Text));
    Prepared prepared =
        new Prepared(
            Utils.STATEMENT_ID,
            Utils.RESULT_METADATA_ID,
            resultMetadata,
            Utils.makePreparedMetadata(Column.create("key", Type.Text)),
            true,
            false);
    when(connection.prepare(eq(query), any(Parameters.class)))
        .thenReturn(CompletableFuture.completedFuture(prepared));

    when(connection.execute(any(Statement.class), any(Parameters.class), anyLong()))
        .thenThrow(new WriteTimeoutException(WriteType.SIMPLE, ConsistencyLevel.QUORUM, 1, 1))
        .then(correctResponse(releaseVersion, resultMetadata, prepared));

    when(persistence.newConnection()).thenReturn(connection);

    startServer(persistence);

    StargateBridgeBlockingStub stub = makeBlockingStub();

    assertThatThrownBy(() -> executeQuery(stub, query, Values.of("local")))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("Operation timed out - received only 1 responses");
  }

  @Test
  public void shouldNotRetryOnWriteTimeoutIfWriteTypeBatchLogButNonIdempotent() {
    final String query = "SELECT release_version FROM system.local WHERE key = ?";
    final String releaseVersion = "4.0.0";

    ResultMetadata resultMetadata =
        Utils.makeResultMetadata(Column.create("release_version", Type.Text));
    Prepared prepared =
        new Prepared(
            Utils.STATEMENT_ID,
            Utils.RESULT_METADATA_ID,
            resultMetadata,
            Utils.makePreparedMetadata(Column.create("key", Type.Text)),
            false,
            false);
    when(connection.prepare(eq(query), any(Parameters.class)))
        .thenReturn(CompletableFuture.completedFuture(prepared));

    when(connection.execute(any(Statement.class), any(Parameters.class), anyLong()))
        .thenThrow(new WriteTimeoutException(WriteType.BATCH_LOG, ConsistencyLevel.QUORUM, 1, 1))
        .then(correctResponse(releaseVersion, resultMetadata, prepared));

    when(persistence.newConnection()).thenReturn(connection);

    startServer(persistence);

    StargateBridgeBlockingStub stub = makeBlockingStub();

    assertThatThrownBy(() -> executeQuery(stub, query, Values.of("local")))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("Operation timed out - received only 1 responses");
  }

  private void validateResponse(String releaseVersion, QueryOuterClass.Response response)
      throws InvalidProtocolBufferException {
    ResultSet rs = response.getResultSet();
    assertThat(rs.getRowsCount()).isEqualTo(1);
    assertThat(rs.getRows(0).getValuesCount()).isEqualTo(1);
    assertThat(rs.getRows(0).getValues(0).getString()).isEqualTo(releaseVersion);
  }

  @NotNull
  private Answer<Object> correctResponse(
      String releaseVersion, ResultMetadata resultMetadata, Prepared prepared) {
    return invocation -> {
      BoundStatement statement = (BoundStatement) invocation.getArgument(0, Statement.class);
      assertStatement(prepared, statement, Values.of("local"));
      List<List<ByteBuffer>> rows =
          Collections.singletonList(
              Collections.singletonList(
                  TypeCodecs.TEXT.encode(releaseVersion, ProtocolVersion.DEFAULT)));
      return CompletableFuture.completedFuture(new Result.Rows(rows, resultMetadata));
    };
  }

  @NotNull
  private QueryOuterClass.Batch createBatch() {
    return QueryOuterClass.Batch.newBuilder()
        .addQueries(
            cqlBatchQuery("INSERT INTO test (k, v) VALUES (?, ?)", Values.of("a"), Values.of(1)))
        .addQueries(
            cqlBatchQuery("INSERT INTO test (k, v) VALUES (?, ?)", Values.of("b"), Values.of(2)))
        .addQueries(
            cqlBatchQuery("INSERT INTO test (k, v) VALUES (?, ?)", Values.of("c"), Values.of(3)))
        .build();
  }

  @NotNull
  private Answer<Object> correctBatchResponse(Prepared prepared) {
    return invocation -> {
      Batch batch = invocation.getArgument(0, Batch.class);

      assertThat(batch.type()).isEqualTo(BatchType.LOGGED);
      assertThat(batch.size()).isEqualTo(3);

      assertStatement(prepared, batch.statements().get(0), Values.of("a"), Values.of(1));
      assertStatement(prepared, batch.statements().get(1), Values.of("b"), Values.of(2));
      assertStatement(prepared, batch.statements().get(2), Values.of("c"), Values.of(3));

      return CompletableFuture.completedFuture(new Result.Void());
    };
  }

  @Test
  public void shouldNotRetryBatchRequestOnWriteTimeoutWhenNonIdempotent() {
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
        .thenThrow(new WriteTimeoutException(WriteType.BATCH_LOG, ConsistencyLevel.QUORUM, 3, 3))
        .then(correctBatchResponse(prepared));

    when(persistence.newConnection()).thenReturn(connection);

    startServer(persistence);

    StargateBridgeBlockingStub stub = makeBlockingStub();

    assertThatThrownBy(() -> stub.executeBatch(createBatch()))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining(
            "DEADLINE_EXCEEDED: Operation timed out - received only 3 responses.");
  }
}
