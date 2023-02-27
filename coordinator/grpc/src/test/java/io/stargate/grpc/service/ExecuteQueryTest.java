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
import io.grpc.Status;
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
import io.stargate.proto.QueryOuterClass.ColumnSpec;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.QueryParameters;
import io.stargate.proto.QueryOuterClass.ResultSet;
import io.stargate.proto.QueryOuterClass.TypeSpec;
import io.stargate.proto.QueryOuterClass.TypeSpec.Basic;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.StargateGrpc.StargateBlockingStub;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Stream;
import org.apache.cassandra.stargate.exceptions.UnhandledClientException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ExecuteQueryTest extends BaseGrpcServiceTest {
  @Test
  public void simpleQuery() {
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
        .then(
            invocation -> {
              BoundStatement statement =
                  (BoundStatement) invocation.getArgument(0, Statement.class);
              assertStatement(prepared, statement, Values.of("local"));
              List<List<ByteBuffer>> rows =
                  Collections.singletonList(
                      Collections.singletonList(
                          TypeCodecs.TEXT.encode(releaseVersion, ProtocolVersion.DEFAULT)));
              return CompletableFuture.completedFuture(new Result.Rows(rows, resultMetadata));
            });

    when(persistence.newConnection()).thenReturn(connection);

    startServer(persistence);

    StargateBlockingStub stub = makeBlockingStub();

    QueryOuterClass.Response response = executeQuery(stub, query, Values.of("local"));

    validateResponse(releaseVersion, response);
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

    QueryOuterClass.Response response =
        stub.executeQuery(
            Query.newBuilder()
                .setCql("INSERT INTO test (c1, c2) VALUE (?, ?)")
                .setParameters(QueryParameters.newBuilder().build())
                .build()); // No payload

    assertThat(response.hasResultSet()).isTrue();
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
              QueryOuterClass.Response response = executeQuery(stub, "DOES NOT MATTER", values);
              assertThat(response).isNotNull(); // Never going to happen
            })
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining(expectedMessage);
  }

  public static Stream<Arguments> invalidValues() {
    return Stream.of(
        // Invalid arity
        arguments(
            org.assertj.core.util.Arrays.array(
                Column.create("k", Type.Text), Column.create("v", Type.Int)),
            org.assertj.core.util.Arrays.array(Values.of("a")),
            "Invalid number of bind values. Expected 2, but received 1"),
        // Invalid type
        arguments(
            org.assertj.core.util.Arrays.array(Column.create("k", Type.Text)),
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

    QueryOuterClass.Response response =
        stub.executeQuery(
            Query.newBuilder()
                .setCql("INSERT INTO test (c1, c2) VALUE (1, 'a')")
                .setParameters(QueryParameters.newBuilder().build())
                .build());

    assertThat(response.getWarningsList()).containsAll(expectedWarnings);
  }

  @ParameterizedTest
  @MethodSource("columnMetadataValues")
  public void columnMetadata(
      ResultMetadata resultMetadata, List<ColumnSpec> expected, boolean skipMetadata) {
    Prepared prepared = Utils.makePrepared();

    when(connection.prepare(anyString(), any(Parameters.class)))
        .thenReturn(CompletableFuture.completedFuture(prepared));

    when(connection.execute(any(Statement.class), any(Parameters.class), anyLong()))
        .thenReturn(
            CompletableFuture.completedFuture(
                new Result.Rows(Collections.emptyList(), resultMetadata)));

    when(persistence.newConnection()).thenReturn(connection);

    startServer(persistence);

    StargateBlockingStub stub = makeBlockingStub();

    QueryOuterClass.Response response =
        stub.executeQuery(
            Query.newBuilder()
                .setCql("INSERT INTO test (c1, c2) VALUE (1, 'a')")
                .setParameters(QueryParameters.newBuilder().setSkipMetadata(skipMetadata).build())
                .build());

    assertThat(response.hasResultSet()).isTrue();
    ResultSet rs = response.getResultSet();
    assertThat(rs.getColumnsList()).containsExactlyElementsOf(expected);
  }

  public static Stream<Arguments> columnMetadataValues() {
    return Stream.of(
        ColumnMetadataBuilder.builder()
            .addActual(Column.create("c1", Column.Type.Int))
            .addActual(Column.create("c2", Column.Type.Text))
            .addActual(Column.create("c3", Column.Type.Uuid))
            .addExpected(
                ColumnSpec.newBuilder()
                    .setName("c1")
                    .setType(TypeSpec.newBuilder().setBasic(TypeSpec.Basic.INT)))
            .addExpected(
                ColumnSpec.newBuilder()
                    .setName("c2")
                    .setType(TypeSpec.newBuilder().setBasic(TypeSpec.Basic.VARCHAR)))
            .addExpected(
                ColumnSpec.newBuilder()
                    .setName("c3")
                    .setType(TypeSpec.newBuilder().setBasic(TypeSpec.Basic.UUID)))
            .build(false),
        ColumnMetadataBuilder.builder()
            .addActual(Column.create("l", Type.List.of(Type.Text)))
            .addExpected(
                ColumnSpec.newBuilder()
                    .setName("l")
                    .setType(
                        TypeSpec.newBuilder()
                            .setList(
                                TypeSpec.List.newBuilder()
                                    .setElement(TypeSpec.newBuilder().setBasic(Basic.VARCHAR)))))
            .build(false),
        ColumnMetadataBuilder.builder()
            .addActual(Column.create("l", Type.List.of(Type.Int)))
            .addExpected(
                ColumnSpec.newBuilder()
                    .setName("l")
                    .setType(
                        TypeSpec.newBuilder()
                            .setList(
                                TypeSpec.List.newBuilder()
                                    .setElement(
                                        TypeSpec.newBuilder().setBasic(TypeSpec.Basic.INT)))))
            .build(false),
        ColumnMetadataBuilder.builder()
            .addActual(Column.create("s", Type.Set.of(Type.Uuid)))
            .addExpected(
                ColumnSpec.newBuilder()
                    .setName("s")
                    .setType(
                        TypeSpec.newBuilder()
                            .setSet(
                                TypeSpec.Set.newBuilder()
                                    .setElement(
                                        TypeSpec.newBuilder().setBasic(TypeSpec.Basic.UUID)))))
            .build(false),
        ColumnMetadataBuilder.builder()
            .addActual(Column.create("m", Type.Map.of(Type.Text, Type.Bigint)))
            .addExpected(
                ColumnSpec.newBuilder()
                    .setName("m")
                    .setType(
                        TypeSpec.newBuilder()
                            .setMap(
                                TypeSpec.Map.newBuilder()
                                    .setKey(TypeSpec.newBuilder().setBasic(TypeSpec.Basic.VARCHAR))
                                    .setValue(
                                        TypeSpec.newBuilder().setBasic(TypeSpec.Basic.BIGINT)))))
            .build(false),
        ColumnMetadataBuilder.builder()
            .addActual(Column.create("c1", Column.Type.Int))
            .addActual(Column.create("c2", Column.Type.Text))
            .addActual(Column.create("c3", Column.Type.Uuid))
            .build(true));
  }

  @Test
  public void unhandledClientExceptionDuringPrepare() {
    when(connection.prepare(anyString(), any(Parameters.class)))
        .thenThrow(new UnhandledClientException(""));

    when(persistence.newConnection()).thenReturn(connection);

    startServer(persistence);

    StargateBlockingStub stub = makeBlockingStub();

    assertThatThrownBy(
            () -> {
              QueryOuterClass.Response response =
                  stub.executeQuery(
                      Query.newBuilder()
                          .setCql("INSERT INTO test (c1, c2) VALUE (1, 'a')")
                          .build());
            })
        .isInstanceOf(StatusRuntimeException.class)
        .extracting("status")
        .extracting("code")
        .isEqualTo(Status.UNAVAILABLE.getCode());
  }

  @Test
  public void unhandledClientExceptionDuringExecute() {
    Prepared prepared = Utils.makePrepared();

    when(connection.prepare(anyString(), any(Parameters.class)))
        .thenReturn(CompletableFuture.completedFuture(prepared));

    when(connection.execute(any(Statement.class), any(Parameters.class), anyLong()))
        .thenThrow(new UnhandledClientException(""));

    when(persistence.newConnection()).thenReturn(connection);

    startServer(persistence);

    StargateBlockingStub stub = makeBlockingStub();

    assertThatThrownBy(
            () -> {
              QueryOuterClass.Response response =
                  stub.executeQuery(
                      Query.newBuilder()
                          .setCql("INSERT INTO test (c1, c2) VALUE (1, 'a')")
                          .build());
            })
        .isInstanceOf(StatusRuntimeException.class)
        .extracting("status")
        .extracting("code")
        .isEqualTo(Status.UNAVAILABLE.getCode());
  }

  @Test
  public void unhandledClientExceptionWrapped() {
    Prepared prepared = Utils.makePrepared();

    when(connection.prepare(anyString(), any(Parameters.class)))
        .thenReturn(CompletableFuture.completedFuture(prepared));

    when(connection.execute(any(Statement.class), any(Parameters.class), anyLong()))
        .thenThrow(new CompletionException(new UnhandledClientException("")));

    when(persistence.newConnection()).thenReturn(connection);

    startServer(persistence);

    StargateBlockingStub stub = makeBlockingStub();

    assertThatThrownBy(
            () -> {
              QueryOuterClass.Response response =
                  stub.executeQuery(
                      Query.newBuilder()
                          .setCql("INSERT INTO test (c1, c2) VALUE (1, 'a')")
                          .build());
            })
        .isInstanceOf(StatusRuntimeException.class)
        .extracting("status")
        .extracting("code")
        .isEqualTo(Status.UNAVAILABLE.getCode());
  }

  private void validateResponse(String releaseVersion, QueryOuterClass.Response response) {
    assertThat(response.hasResultSet()).isTrue();
    ResultSet rs = response.getResultSet();
    assertThat(rs.getRowsCount()).isEqualTo(1);
    assertThat(rs.getRows(0).getValuesCount()).isEqualTo(1);
    assertThat(rs.getRows(0).getValues(0).getString()).isEqualTo(releaseVersion);
  }

  private static class ColumnMetadataBuilder {
    private final List<Column> actual = new ArrayList<>();
    private final List<ColumnSpec> expected = new ArrayList<>();

    public static ColumnMetadataBuilder builder() {
      return new ColumnMetadataBuilder();
    }

    public ColumnMetadataBuilder addActual(Column column) {
      actual.add(column);
      return this;
    }

    public ColumnMetadataBuilder addExpected(ColumnSpec.Builder column) {
      expected.add(column.build());
      return this;
    }

    Arguments build(boolean skipMetadata) {
      return arguments(
          Utils.makeResultMetadata(actual.toArray(new Column[0])), expected, skipMetadata);
    }
  }
}
