package io.stargate.grpc.service;

import static io.stargate.grpc.Values.intValue;
import static io.stargate.grpc.Values.stringValue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import io.grpc.StatusRuntimeException;
import io.stargate.db.Batch;
import io.stargate.db.BatchType;
import io.stargate.db.Parameters;
import io.stargate.db.Result;
import io.stargate.db.Result.Prepared;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.Type;
import io.stargate.grpc.Utils;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.StargateGrpc.StargateBlockingStub;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.assertj.core.util.Arrays;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

public class ExecuteBatchTest extends BaseServiceTest {

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
            Utils.makePreparedMetadata(
                Column.create("k", Type.Varchar), Column.create("v", Type.Int)));
    when(connection.prepare(anyString(), any(Parameters.class)))
        .thenReturn(CompletableFuture.completedFuture(prepared));

    when(connection.batch(any(Batch.class), any(Parameters.class), anyLong()))
        .then(
            invocation -> {
              Batch batch = invocation.getArgument(0, Batch.class);

              assertThat(batch.type()).isEqualTo(BatchType.LOGGED);
              assertThat(batch.size()).isEqualTo(3);

              assertStatement(prepared, batch.statements().get(0), stringValue("a"), intValue(1));
              assertStatement(prepared, batch.statements().get(1), stringValue("b"), intValue(2));
              assertStatement(prepared, batch.statements().get(2), stringValue("c"), intValue(3));

              return CompletableFuture.completedFuture(new Result.Void());
            });

    when(persistence.newConnection()).thenReturn(connection);

    startServer(persistence);

    StargateBlockingStub stub = makeBlockingStub();

    QueryOuterClass.Result result =
        stub.executeBatch(
            QueryOuterClass.Batch.newBuilder()
                .addQueries(
                    cqlBatchQuery(
                        "INSERT INTO test (k, v) VALUES (?, ?)", stringValue("a"), intValue(1)))
                .addQueries(
                    cqlBatchQuery(
                        "INSERT INTO test (k, v) VALUES (?, ?)", stringValue("b"), intValue(2)))
                .addQueries(
                    cqlBatchQuery(
                        "INSERT INTO test (k, v) VALUES (?, ?)", stringValue("c"), intValue(3)))
                .build());

    assertThat(result.hasPayload()).isFalse();
  }

  @ParameterizedTest
  @EnumSource(BatchType.class)
  public void batchTypes(BatchType type) {
    Prepared prepared =
        new Prepared(
            Utils.STATEMENT_ID,
            Utils.RESULT_METADATA_ID,
            Utils.makeResultMetadata(),
            Utils.makePreparedMetadata());
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

    QueryOuterClass.Result result =
        stub.executeBatch(
            QueryOuterClass.Batch.newBuilder()
                .setType(QueryOuterClass.Batch.Type.forNumber(type.id))
                .addQueries(cqlBatchQuery("INSERT INTO test (k, v) VALUES ('a', 1)"))
                .build());

    assertThat(result.hasPayload()).isFalse();
  }

  @Test
  public void noQueries() {
    when(persistence.newConnection()).thenReturn(connection);

    startServer(persistence);

    StargateBlockingStub stub = makeBlockingStub();

    assertThatThrownBy(
            () -> {
              QueryOuterClass.Result result =
                  stub.executeBatch(QueryOuterClass.Batch.newBuilder().build());
              assertThat(result.hasPayload()).isFalse();
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
            Utils.makePreparedMetadata(columns));
    when(connection.prepare(anyString(), any(Parameters.class)))
        .thenReturn(CompletableFuture.completedFuture(prepared));

    when(persistence.newConnection()).thenReturn(connection);

    startServer(persistence);

    StargateBlockingStub stub = makeBlockingStub();

    assertThatThrownBy(
            () -> {
              QueryOuterClass.Result result =
                  stub.executeBatch(
                      QueryOuterClass.Batch.newBuilder()
                          .addQueries(cqlBatchQuery("DOES NOT MATTER", values))
                          .build());
              assertThat(result).isNotNull(); // Never going to happen
            })
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining(expectedMessage);
  }

  public static Stream<Arguments> invalidValues() {
    return Stream.of(
        // Invalid arity
        arguments(
            Arrays.array(Column.create("k", Type.Varchar), Column.create("v", Type.Int)),
            Arrays.array(stringValue("a")),
            "Invalid number of bind values. Expected 2, but received 1"),
        // Invalid type
        arguments(
            Arrays.array(Column.create("k", Type.Varchar)),
            Arrays.array(intValue(1)),
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

    QueryOuterClass.Result result =
        stub.executeBatch(
            QueryOuterClass.Batch.newBuilder()
                .addQueries(cqlBatchQuery("INSERT INTO test (k, v) VALUES ('a', 1)"))
                .build());

    assertThat(result.getWarningsList()).containsAll(expectedWarnings);
  }
}
