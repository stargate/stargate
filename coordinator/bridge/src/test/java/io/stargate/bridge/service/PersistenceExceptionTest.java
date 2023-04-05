package io.stargate.bridge.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.stargate.bridge.Utils;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.QueryOuterClass.AlreadyExists;
import io.stargate.bridge.proto.QueryOuterClass.CasWriteUnknown;
import io.stargate.bridge.proto.QueryOuterClass.Consistency;
import io.stargate.bridge.proto.QueryOuterClass.FunctionFailure;
import io.stargate.bridge.proto.QueryOuterClass.ReadFailure;
import io.stargate.bridge.proto.QueryOuterClass.ReadTimeout;
import io.stargate.bridge.proto.QueryOuterClass.Unavailable;
import io.stargate.bridge.proto.QueryOuterClass.WriteFailure;
import io.stargate.bridge.proto.QueryOuterClass.WriteTimeout;
import io.stargate.bridge.proto.StargateBridgeGrpc.StargateBridgeBlockingStub;
import io.stargate.db.Parameters;
import io.stargate.db.Result;
import io.stargate.db.Statement;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.apache.cassandra.stargate.cql3.functions.FunctionName;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.apache.cassandra.stargate.db.WriteType;
import org.apache.cassandra.stargate.exceptions.AlreadyExistsException;
import org.apache.cassandra.stargate.exceptions.AuthenticationException;
import org.apache.cassandra.stargate.exceptions.CDCWriteException;
import org.apache.cassandra.stargate.exceptions.CasWriteUnknownResultException;
import org.apache.cassandra.stargate.exceptions.ConfigurationException;
import org.apache.cassandra.stargate.exceptions.FunctionExecutionException;
import org.apache.cassandra.stargate.exceptions.InvalidRequestException;
import org.apache.cassandra.stargate.exceptions.IsBootstrappingException;
import org.apache.cassandra.stargate.exceptions.OverloadedException;
import org.apache.cassandra.stargate.exceptions.PersistenceException;
import org.apache.cassandra.stargate.exceptions.PreparedQueryNotFoundException;
import org.apache.cassandra.stargate.exceptions.ReadFailureException;
import org.apache.cassandra.stargate.exceptions.ReadTimeoutException;
import org.apache.cassandra.stargate.exceptions.RequestFailureReason;
import org.apache.cassandra.stargate.exceptions.SyntaxException;
import org.apache.cassandra.stargate.exceptions.TruncateException;
import org.apache.cassandra.stargate.exceptions.UnauthorizedException;
import org.apache.cassandra.stargate.exceptions.UnavailableException;
import org.apache.cassandra.stargate.exceptions.WriteFailureException;
import org.apache.cassandra.stargate.exceptions.WriteTimeoutException;
import org.apache.cassandra.stargate.locator.InetAddressAndPort;
import org.apache.cassandra.stargate.transport.ProtocolException;
import org.apache.cassandra.stargate.transport.ServerError;
import org.apache.cassandra.stargate.utils.MD5Digest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class PersistenceExceptionTest extends BaseBridgeServiceTest {

  private static final MD5Digest UNPREPARED_ID = MD5Digest.compute(new byte[] {0});

  @Test
  public void unavailable() {
    assertThatThrownBy(
            () ->
                executeQueryWithException(
                    UnavailableException.create(ConsistencyLevel.QUORUM, 2, 1)))
        .isInstanceOf(StatusRuntimeException.class)
        .satisfies(
            ex -> {
              StatusRuntimeException se = (StatusRuntimeException) ex;
              assertThat(se.getStatus().getCode()).isEqualTo(Status.UNAVAILABLE.getCode());
              assertThat(se.getTrailers()).isNotNull();
              Unavailable metadata = se.getTrailers().get(ExceptionHandler.UNAVAILABLE_KEY);
              assertThat(metadata).isNotNull();
              assertThat(metadata.getConsistency()).isEqualTo(Consistency.QUORUM);
              assertThat(metadata.getRequired()).isEqualTo(2);
              assertThat(metadata.getAlive()).isEqualTo(1);
            })
        .hasMessageContaining("Cannot achieve consistency level ");
  }

  @Test
  public void writeTimeout() {
    assertThatThrownBy(
            () ->
                executeQueryWithException(
                    new WriteTimeoutException(
                        WriteType.BATCH, ConsistencyLevel.LOCAL_QUORUM, 1, 2)))
        .isInstanceOf(StatusRuntimeException.class)
        .satisfies(
            ex -> {
              StatusRuntimeException se = (StatusRuntimeException) ex;
              assertThat(se.getStatus().getCode()).isEqualTo(Status.DEADLINE_EXCEEDED.getCode());
              assertThat(se.getTrailers()).isNotNull();
              WriteTimeout metadata = se.getTrailers().get(ExceptionHandler.WRITE_TIMEOUT_KEY);
              assertThat(metadata).isNotNull();
              assertThat(metadata.getConsistency()).isEqualTo(Consistency.LOCAL_QUORUM);
              assertThat(metadata.getWriteType()).isEqualTo("BATCH");
              assertThat(metadata.getReceived()).isEqualTo(1);
              assertThat(metadata.getBlockFor()).isEqualTo(2);
            })
        .hasMessageContaining("Operation timed out - received only 1 responses.");
  }

  @Test
  public void readTimeout() {
    assertThatThrownBy(
            () ->
                executeQueryWithException(
                    new ReadTimeoutException(ConsistencyLevel.LOCAL_QUORUM, 1, 2, true)))
        .isInstanceOf(StatusRuntimeException.class)
        .satisfies(
            ex -> {
              StatusRuntimeException se = (StatusRuntimeException) ex;
              assertThat(se.getStatus().getCode()).isEqualTo(Status.DEADLINE_EXCEEDED.getCode());
              assertThat(se.getTrailers()).isNotNull();
              ReadTimeout metadata = se.getTrailers().get(ExceptionHandler.READ_TIMEOUT_KEY);
              assertThat(metadata).isNotNull();
              assertThat(metadata.getConsistency()).isEqualTo(Consistency.LOCAL_QUORUM);
              assertThat(metadata.getReceived()).isEqualTo(1);
              assertThat(metadata.getBlockFor()).isEqualTo(2);
              assertThat(metadata.getDataPresent()).isTrue();
            })
        .hasMessageContaining("Operation timed out - received only 1 responses.");
  }

  @Test
  public void readFailure() {
    assertThatThrownBy(
            () ->
                executeQueryWithException(
                    new ReadFailureException(
                        ConsistencyLevel.TWO,
                        0,
                        2,
                        false,
                        ImmutableMap.of(
                            InetAddressAndPort.getByName("127.0.0.1"),
                            RequestFailureReason.TIMEOUT,
                            InetAddressAndPort.getByName("127.0.0.2"),
                            RequestFailureReason.INCOMPATIBLE_SCHEMA))))
        .isInstanceOf(StatusRuntimeException.class)
        .satisfies(
            ex -> {
              StatusRuntimeException se = (StatusRuntimeException) ex;
              assertThat(se.getStatus().getCode()).isEqualTo(Status.ABORTED.getCode());
              assertThat(se.getTrailers()).isNotNull();
              ReadFailure metadata = se.getTrailers().get(ExceptionHandler.READ_FAILURE_KEY);
              assertThat(metadata).isNotNull();
              assertThat(metadata.getConsistency()).isEqualTo(Consistency.TWO);
              assertThat(metadata.getReceived()).isEqualTo(0);
              assertThat(metadata.getBlockFor()).isEqualTo(2);
              assertThat(metadata.getNumFailures()).isEqualTo(2);
              assertThat(metadata.getDataPresent()).isFalse();
            })
        .hasMessageContaining("Operation failed - received 0 responses and 2 failures");
  }

  @Test
  public void functionFailure() {
    assertThatThrownBy(
            () ->
                executeQueryWithException(
                    new FunctionExecutionException(
                        new FunctionName("ks", "fn"),
                        ImmutableList.of("int", "varchar"),
                        "Some failure happened")))
        .isInstanceOf(StatusRuntimeException.class)
        .satisfies(
            ex -> {
              StatusRuntimeException se = (StatusRuntimeException) ex;
              assertThat(se.getStatus().getCode()).isEqualTo(Status.FAILED_PRECONDITION.getCode());
              assertThat(se.getTrailers()).isNotNull();
              FunctionFailure metadata =
                  se.getTrailers().get(ExceptionHandler.FUNCTION_FAILURE_KEY);
              assertThat(metadata).isNotNull();
              assertThat(metadata.getKeyspace()).isEqualTo("ks");
              assertThat(metadata.getFunction()).isEqualTo("fn");
              assertThat(metadata.getArgTypesList()).containsExactly("int", "varchar");
            })
        .hasMessageContaining("execution of 'ks.fn[int, varchar]' failed: Some failure happened");
  }

  @Test
  public void writeFailure() {
    assertThatThrownBy(
            () ->
                executeQueryWithException(
                    new WriteFailureException(
                        ConsistencyLevel.THREE,
                        1,
                        3,
                        WriteType.SIMPLE,
                        ImmutableMap.of(
                            InetAddressAndPort.getByName("127.0.0.1"),
                            RequestFailureReason.TIMEOUT,
                            InetAddressAndPort.getByName("127.0.0.2"),
                            RequestFailureReason.INCOMPATIBLE_SCHEMA))))
        .isInstanceOf(StatusRuntimeException.class)
        .satisfies(
            ex -> {
              StatusRuntimeException se = (StatusRuntimeException) ex;
              assertThat(se.getStatus().getCode()).isEqualTo(Status.ABORTED.getCode());
              assertThat(se.getTrailers()).isNotNull();
              WriteFailure metadata = se.getTrailers().get(ExceptionHandler.WRITE_FAILURE_KEY);
              assertThat(metadata).isNotNull();
              assertThat(metadata.getConsistency()).isEqualTo(Consistency.THREE);
              assertThat(metadata.getReceived()).isEqualTo(1);
              assertThat(metadata.getBlockFor()).isEqualTo(3);
              assertThat(metadata.getNumFailures()).isEqualTo(2);
            })
        .hasMessageContaining("Operation failed - received 1 responses and 2 failures");
  }

  @Test
  public void alreadyExists() {
    assertThatThrownBy(() -> executeQueryWithException(new AlreadyExistsException("ks", "table")))
        .isInstanceOf(StatusRuntimeException.class)
        .satisfies(
            ex -> {
              StatusRuntimeException se = (StatusRuntimeException) ex;
              assertThat(se.getStatus().getCode()).isEqualTo(Status.ALREADY_EXISTS.getCode());
              assertThat(se.getTrailers()).isNotNull();
              AlreadyExists metadata = se.getTrailers().get(ExceptionHandler.ALREADY_EXISTS_KEY);
              assertThat(metadata).isNotNull();
              assertThat(metadata.getKeyspace()).isEqualTo("ks");
              assertThat(metadata.getTable()).isEqualTo("table");
            })
        .hasMessageContaining("Cannot add already existing table \"table\" to keyspace \"ks\"");
  }

  @Test
  public void casWriteUnknown() {
    assertThatThrownBy(
            () ->
                executeQueryWithException(
                    new CasWriteUnknownResultException(ConsistencyLevel.SERIAL, 2, 3)))
        .isInstanceOf(StatusRuntimeException.class)
        .satisfies(
            ex -> {
              StatusRuntimeException se = (StatusRuntimeException) ex;
              assertThat(se.getStatus().getCode()).isEqualTo(Status.ABORTED.getCode());
              assertThat(se.getTrailers()).isNotNull();
              CasWriteUnknown metadata =
                  se.getTrailers().get(ExceptionHandler.CAS_WRITE_UNKNOWN_KEY);
              assertThat(metadata).isNotNull();
              assertThat(metadata.getConsistency()).isEqualTo(Consistency.SERIAL);
              assertThat(metadata.getReceived()).isEqualTo(2);
              assertThat(metadata.getBlockFor()).isEqualTo(3);
            })
        .hasMessageContaining(
            "CAS operation result is unknown - proposal accepted by 2 but not a quorum.");
  }

  @Test
  public void unpreparedRetrySuccess() {
    // 2 retries, so test both retry and one success after
    when(persistence.newConnection()).thenReturn(connection);
    when(connection.prepare(anyString(), any(Parameters.class)))
        .thenReturn(CompletableFuture.completedFuture(Utils.makePrepared()));
    when(connection.execute(any(Statement.class), any(Parameters.class), anyLong()))
        .then(
            invocation -> {
              throw new PreparedQueryNotFoundException(UNPREPARED_ID);
            })
        .then(
            invocation -> {
              throw new PreparedQueryNotFoundException(UNPREPARED_ID);
            })
        .then(
            invocation -> {
              Result result = new Result.Void();
              return CompletableFuture.completedFuture(result);
            });
    startServer(persistence);

    StargateBridgeBlockingStub stub = makeBlockingStub();
    QueryOuterClass.Response response = executeQuery(stub, "DOESN'T MATTER");

    assertThat(response).isNotNull();
  }

  @ParameterizedTest
  @MethodSource("persistenceExceptionValues")
  public void persistenceException(
      PersistenceException pe, Status expectedStatus, String expectedMessage) {
    assertThatThrownBy(() -> executeQueryWithException(pe))
        .isInstanceOf(StatusRuntimeException.class)
        .satisfies(
            ex -> {
              StatusRuntimeException se = (StatusRuntimeException) ex;
              assertThat(se.getStatus().getCode()).isEqualTo(expectedStatus.getCode());
            })
        .hasMessageContaining(expectedMessage);
  }

  public static Stream<Arguments> persistenceExceptionValues() {
    return Stream.of(
        Arguments.of(
            new ServerError("Some internal problem"), Status.INTERNAL, "Some internal problem"),
        Arguments.of(
            new ProtocolException("Some protocol problem"),
            Status.INTERNAL,
            "Some protocol problem"),
        Arguments.of(
            new PreparedQueryNotFoundException(UNPREPARED_ID),
            Status.INTERNAL,
            String.format("Prepared query with ID %s not found", UNPREPARED_ID)),
        Arguments.of(
            new InvalidRequestException("Some request problem"),
            Status.INVALID_ARGUMENT,
            "Some request problem"),
        Arguments.of(
            new SyntaxException("Some query syntax problem"),
            Status.INVALID_ARGUMENT,
            "Some query syntax problem"),
        Arguments.of(
            new TruncateException("Some truncate problem"),
            Status.ABORTED,
            "Some truncate problem"),
        Arguments.of(
            new CDCWriteException("Some CDC write problem"),
            Status.ABORTED,
            "Some CDC write problem"),
        Arguments.of(
            new AuthenticationException("Some credentials problem"),
            Status.UNAUTHENTICATED,
            "Some credentials problem"),
        Arguments.of(
            new OverloadedException("Some overloaded problem"),
            Status.RESOURCE_EXHAUSTED,
            "Some overloaded problem"),
        Arguments.of(
            new IsBootstrappingException(),
            Status.UNAVAILABLE,
            "Cannot read from a bootstrapping node"),
        Arguments.of(
            new ServerError("Some internal problem"), Status.INTERNAL, "Some internal problem"),
        Arguments.of(
            new UnauthorizedException("Some authorization problem"),
            Status.PERMISSION_DENIED,
            "Some authorization problem"),
        Arguments.of(
            new ConfigurationException("Some config problem"),
            Status.FAILED_PRECONDITION,
            "Some config problem"));
  }

  private void executeQueryWithException(PersistenceException pe) {
    when(connection.prepare(anyString(), any(Parameters.class)))
        .thenReturn(CompletableFuture.completedFuture(Utils.makePrepared()));

    when(connection.execute(any(Statement.class), any(Parameters.class), anyLong()))
        .then(
            invocation -> {
              throw pe;
            });

    when(persistence.newConnection()).thenReturn(connection);

    startServer(persistence);

    StargateBridgeBlockingStub stub = makeBlockingStub();

    QueryOuterClass.Response response = executeQuery(stub, "DOESN'T MATTER");
    assertThat(response).isNotNull();
  }
}
