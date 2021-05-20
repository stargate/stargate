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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.StatusRuntimeException;
import io.stargate.db.BoundStatement;
import io.stargate.db.Parameters;
import io.stargate.db.Result;
import io.stargate.db.Result.Prepared;
import io.stargate.db.Result.ResultMetadata;
import io.stargate.db.Statement;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.Type;
import io.stargate.grpc.Utils;
import io.stargate.grpc.Values;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.QueryOuterClass.Payload;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.QueryParameters;
import io.stargate.proto.QueryOuterClass.ResultSet;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.StargateGrpc.StargateBlockingStub;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ExecuteQueryTest extends BaseServiceTest {
  @Test
  public void simpleQuery() throws InvalidProtocolBufferException {
    final String query = "SELECT release_version FROM system.local WHERE key = ?";
    final String releaseVersion = "4.0.0";

    ResultMetadata resultMetadata =
        Utils.makeResultMetadata(Column.create("release_version", Type.Varchar));
    Prepared prepared =
        new Prepared(
            Utils.STATEMENT_ID,
            Utils.RESULT_METADATA_ID,
            resultMetadata,
            Utils.makePreparedMetadata(Column.create("key", Type.Varchar)));
    when(connection.prepare(eq(query), any(Parameters.class)))
        .thenReturn(CompletableFuture.completedFuture(prepared));

    when(connection.execute(any(Statement.class), any(Parameters.class), anyLong()))
        .then(
            invocation -> {
              BoundStatement statement =
                  (BoundStatement) invocation.getArgument(0, Statement.class);
              assertStatement(prepared, statement, Values.of("local"));
              List<List<ByteBuffer>> rows =
                  Arrays.asList(
                      Arrays.asList(
                          TypeCodecs.TEXT.encode(releaseVersion, ProtocolVersion.DEFAULT)));
              return CompletableFuture.completedFuture(new Result.Rows(rows, resultMetadata));
            });

    when(persistence.newConnection()).thenReturn(connection);

    startServer(persistence);

    StargateBlockingStub stub = makeBlockingStub();

    QueryOuterClass.Result result = executeQuery(stub, query, Values.of("local"));

    assertThat(result.hasPayload()).isTrue();
    ResultSet rs = result.getPayload().getValue().unpack(ResultSet.class);
    assertThat(rs.getRowsCount()).isEqualTo(1);
    assertThat(rs.getRows(0).getValuesCount()).isEqualTo(1);
    assertThat(rs.getRows(0).getValues(0).getString()).isEqualTo(releaseVersion);
  }

  @Test
  public void noPayload() {
    ResultMetadata resultMetadata = Utils.makeResultMetadata();
    Prepared prepared = Utils.makePrepared();

    when(connection.prepare(anyString(), any(Parameters.class)))
        .thenReturn(CompletableFuture.completedFuture(prepared));

    when(connection.execute(any(Statement.class), any(Parameters.class), anyLong()))
        .thenReturn(
            CompletableFuture.completedFuture(
                new Result.Rows(Collections.emptyList(), resultMetadata))); // Return no payload

    when(persistence.newConnection()).thenReturn(connection);

    startServer(persistence);

    StargateBlockingStub stub = makeBlockingStub();

    QueryOuterClass.Result result =
        stub.executeQuery(
            Query.newBuilder()
                .setCql("INSERT INTO test (c1, c2) VALUE (1, 'a')")
                .setParameters(QueryParameters.newBuilder().build()) // No payload
                .build());

    assertThat(result.hasPayload()).isTrue();
    assertThat(result.getPayload().hasValue()).isFalse();
    assertThat(result.getPayload().getType()).isEqualTo(Payload.Type.TYPE_CQL);
  }

  @ParameterizedTest
  @MethodSource({"invalidValues"})
  public void invalidValuesTest(Column[] columns, Value[] values, String expectedMessage) {
    Prepared prepared =
        new Prepared(
            Utils.STATEMENT_ID,
            Utils.RESULT_METADATA_ID,
            Utils.makeResultMetadata(),
            Utils.makePreparedMetadata(columns));
    when(connection.prepare(anyString(), any(Parameters.class)))
        .thenReturn(CompletableFuture.completedFuture(prepared));

    when(persistence.newConnection()).thenReturn(connection);

    startServer(persistence);

    StargateBlockingStub stub = makeBlockingStub();

    assertThatThrownBy(
            () -> {
              QueryOuterClass.Result result = executeQuery(stub, "DOES NOT MATTER", values);
              assertThat(result).isNotNull(); // Never going to happen
            })
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining(expectedMessage);
  }

  public static Stream<Arguments> invalidValues() {
    return Stream.of(
        // Invalid arity
        arguments(
            org.assertj.core.util.Arrays.array(
                Column.create("k", Type.Varchar), Column.create("v", Type.Int)),
            org.assertj.core.util.Arrays.array(Values.of("a")),
            "Invalid number of bind values. Expected 2, but received 1"),
        // Invalid type
        arguments(
            org.assertj.core.util.Arrays.array(Column.create("k", Type.Varchar)),
            org.assertj.core.util.Arrays.array(Values.of(1)),
            "Invalid argument at position 1"));
  }

  @Test
  public void warnings() {
    ResultMetadata resultMetadata = Utils.makeResultMetadata();
    Prepared prepared = Utils.makePrepared();

    List<String> expectedWarnings = Arrays.asList("warning 1", "warning 2");

    when(connection.prepare(anyString(), any(Parameters.class)))
        .thenReturn(CompletableFuture.completedFuture(prepared));

    when(connection.execute(any(Statement.class), any(Parameters.class), anyLong()))
        .thenReturn(
            CompletableFuture.completedFuture(
                new Result.Rows(Collections.emptyList(), resultMetadata)
                    .setWarnings(expectedWarnings)));

    when(persistence.newConnection()).thenReturn(connection);

    startServer(persistence);

    StargateBlockingStub stub = makeBlockingStub();

    QueryOuterClass.Result result =
        stub.executeQuery(
            Query.newBuilder()
                .setCql("INSERT INTO test (c1, c2) VALUE (1, 'a')")
                .setParameters(QueryParameters.newBuilder().build()) // No payload
                .build());

    assertThat(result.getWarningsList()).containsAll(expectedWarnings);
  }
}
