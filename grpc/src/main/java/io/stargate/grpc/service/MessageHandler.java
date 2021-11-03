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

import static io.stargate.grpc.retries.RetryDecision.RETHROW;

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.StreamObserver;
import io.stargate.db.BoundStatement;
import io.stargate.db.ImmutableParameters;
import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.Persistence.Connection;
import io.stargate.db.Result;
import io.stargate.db.Result.Prepared;
import io.stargate.db.tracing.QueryTracingFetcher;
import io.stargate.grpc.retries.DefaultRetryPolicy;
import io.stargate.grpc.retries.RetryDecision;
import io.stargate.grpc.service.GrpcService.ResponseAndTraceId;
import io.stargate.grpc.tracing.TraceEventsMapper;
import io.stargate.proto.QueryOuterClass.AlreadyExists;
import io.stargate.proto.QueryOuterClass.CasWriteUnknown;
import io.stargate.proto.QueryOuterClass.FunctionFailure;
import io.stargate.proto.QueryOuterClass.ReadFailure;
import io.stargate.proto.QueryOuterClass.ReadTimeout;
import io.stargate.proto.QueryOuterClass.Response;
import io.stargate.proto.QueryOuterClass.Unavailable;
import io.stargate.proto.QueryOuterClass.Values;
import io.stargate.proto.QueryOuterClass.WriteFailure;
import io.stargate.proto.QueryOuterClass.WriteTimeout;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import javax.annotation.Nullable;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.apache.cassandra.stargate.exceptions.AlreadyExistsException;
import org.apache.cassandra.stargate.exceptions.CasWriteUnknownResultException;
import org.apache.cassandra.stargate.exceptions.FunctionExecutionException;
import org.apache.cassandra.stargate.exceptions.PersistenceException;
import org.apache.cassandra.stargate.exceptions.ReadFailureException;
import org.apache.cassandra.stargate.exceptions.ReadTimeoutException;
import org.apache.cassandra.stargate.exceptions.UnavailableException;
import org.apache.cassandra.stargate.exceptions.UnhandledClientException;
import org.apache.cassandra.stargate.exceptions.WriteFailureException;
import org.apache.cassandra.stargate.exceptions.WriteTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @param <MessageT> the type of gRPC message being handled.
 * @param <PreparedT> the persistence object resulting from the preparation of the query(ies).
 */
abstract class MessageHandler<MessageT extends GeneratedMessageV3, PreparedT> {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultRetryPolicy.class);

  static final Metadata.Key<Unavailable> UNAVAILABLE_KEY =
      ProtoUtils.keyForProto(Unavailable.getDefaultInstance());
  static final Metadata.Key<WriteTimeout> WRITE_TIMEOUT_KEY =
      ProtoUtils.keyForProto(WriteTimeout.getDefaultInstance());
  static final Metadata.Key<ReadTimeout> READ_TIMEOUT_KEY =
      ProtoUtils.keyForProto(ReadTimeout.getDefaultInstance());
  static final Metadata.Key<ReadFailure> READ_FAILURE_KEY =
      ProtoUtils.keyForProto(ReadFailure.getDefaultInstance());
  static final Metadata.Key<FunctionFailure> FUNCTION_FAILURE_KEY =
      ProtoUtils.keyForProto(FunctionFailure.getDefaultInstance());
  static final Metadata.Key<WriteFailure> WRITE_FAILURE_KEY =
      ProtoUtils.keyForProto(WriteFailure.getDefaultInstance());
  static final Metadata.Key<AlreadyExists> ALREADY_EXISTS_KEY =
      ProtoUtils.keyForProto(AlreadyExists.getDefaultInstance());
  static final Metadata.Key<CasWriteUnknown> CAS_WRITE_UNKNOWN_KEY =
      ProtoUtils.keyForProto(CasWriteUnknown.getDefaultInstance());

  protected static final ConsistencyLevel DEFAULT_TRACING_CONSISTENCY = ConsistencyLevel.ONE;

  protected final MessageT message;
  protected final Connection connection;
  protected final Persistence persistence;
  private final StreamObserver<Response> responseObserver;
  private final DefaultRetryPolicy retryPolicy;

  protected MessageHandler(
      MessageT message,
      Connection connection,
      Persistence persistence,
      StreamObserver<Response> responseObserver) {
    this.message = message;
    this.connection = connection;
    this.persistence = persistence;
    this.responseObserver = responseObserver;
    this.retryPolicy = new DefaultRetryPolicy();
  }

  void handle() {
    try {
      validate();
      executeWithRetry(0);

    } catch (Throwable t) {
      handleException(t);
    }
  }

  private void executeWithRetry(int retryCount) {
    executeQuery()
        .whenComplete(
            (response, error) -> {
              if (error != null) {
                RetryDecision decision = shouldRetry(error, retryCount);
                switch (decision) {
                  case RETRY:
                    executeWithRetry(retryCount + 1);
                    break;
                  case RETHROW:
                    handleException(error);
                    break;
                  default:
                    throw new UnsupportedOperationException(
                        "The retry decision: " + decision + " is not supported.");
                }
              } else {
                setSuccess(response);
              }
            });
  }

  private CompletionStage<Response> executeQuery() {
    CompletionStage<Result> resultFuture = prepare().thenCompose(this::executePrepared);
    return handleUnprepared(resultFuture)
        .thenCompose(this::buildResponse)
        .thenCompose(this::executeTracingQueryIfNeeded);
  }

  private RetryDecision shouldRetry(Throwable throwable, int retryCount) {
    Optional<PersistenceException> cause = unwrapCause(throwable);
    if (!cause.isPresent()) {
      return RETHROW;
    }
    PersistenceException pe = cause.get();
    switch (pe.code()) {
      case READ_TIMEOUT:
        return retryPolicy.onReadTimeout((ReadTimeoutException) pe, retryCount);
      case WRITE_TIMEOUT:
        if (isIdempotent(throwable)) {
          return retryPolicy.onWriteTimeout((WriteTimeoutException) pe, retryCount);
        } else {
          return RETHROW;
        }
      default:
        return RETHROW;
    }
  }

  private boolean isIdempotent(Throwable throwable) {
    Optional<ExceptionWithIdempotencyInfo> exception =
        unwrapExceptionWithIdempotencyInfo(throwable);
    return exception.map(ExceptionWithIdempotencyInfo::isIdempotent).orElse(false);
  }

  private Optional<ExceptionWithIdempotencyInfo> unwrapExceptionWithIdempotencyInfo(
      Throwable throwable) {
    if (throwable instanceof CompletionException) {
      return unwrapExceptionWithIdempotencyInfo(throwable.getCause());
    } else if (throwable instanceof ExceptionWithIdempotencyInfo) {
      return Optional.of((ExceptionWithIdempotencyInfo) throwable);
    } else {
      return Optional.empty();
    }
  }

  protected Optional<PersistenceException> unwrapCause(Throwable throwable) {
    if (throwable instanceof CompletionException
        || throwable instanceof ExceptionWithIdempotencyInfo) {
      return unwrapCause(throwable.getCause());
    } else if (throwable instanceof StatusException
        || throwable instanceof StatusRuntimeException) {
      return Optional.empty();
    } else if (throwable instanceof PersistenceException) {
      return Optional.of((PersistenceException) throwable);
    } else {
      return Optional.empty();
    }
  }

  /** Performs any necessary validation on the message before execution starts. */
  protected abstract void validate() throws Exception;

  /**
   * Prepares any CQL query required for the execution of the request, and returns an executable
   * object.
   */
  protected abstract CompletionStage<PreparedT> prepare();

  /** Executes the prepared object to get the CQL results. */
  protected abstract CompletionStage<Result> executePrepared(PreparedT prepared);

  /** Builds the gRPC response from the CQL result. */
  protected abstract CompletionStage<ResponseAndTraceId> buildResponse(Result result);

  /** Computes the consistency level to use for tracing queries. */
  protected abstract ConsistencyLevel getTracingConsistency();

  protected BoundStatement bindValues(Prepared prepared, Values values) throws Exception {
    return values.getValuesCount() > 0
        ? ValuesHelper.bindValues(prepared, values, persistence.unsetValue())
        : new BoundStatement(prepared.statementId, Collections.emptyList(), null);
  }

  protected CompletionStage<Prepared> prepare(String cql, @Nullable String keyspace) {
    return maybePrepared(cql, keyspace)
        .thenApply(
            prepared -> {
              if (prepared.isUseKeyspace) {
                throw Status.INVALID_ARGUMENT
                    .withDescription("USE <keyspace> not supported")
                    .asRuntimeException();
              }
              return prepared;
            });
  }

  private CompletionStage<Prepared> maybePrepared(String cql, @Nullable String keyspace) {
    Parameters parameters =
        (keyspace == null)
            ? Parameters.defaults()
            : ImmutableParameters.builder().defaultKeyspace(keyspace).build();

    Prepared preparedInCache = connection.getPrepared(cql, parameters);
    return preparedInCache != null
        ? CompletableFuture.completedFuture(preparedInCache)
        : connection.prepare(cql, parameters);
  }

  /**
   * If our local prepared statement cache gets out of sync with the server, we might get an
   * UNPREPARED response when executing a query. This method allows us to recover from that case
   * (other execution errors get propagated as-is).
   */
  private CompletionStage<Result> handleUnprepared(CompletionStage<Result> source) {
    CompletableFuture<Result> target = new CompletableFuture<>();
    source.whenComplete(
        (result, error) -> {
          if (error != null) {
            if (error instanceof CompletionException) {
              error = error.getCause();
            }
            target.completeExceptionally(error);
          } else {
            target.complete(result);
          }
        });
    return target;
  }

  protected Response.Builder makeResponseBuilder(Result result) {
    Response.Builder resultBuilder = Response.newBuilder();
    List<String> warnings = result.getWarnings();
    if (warnings != null) {
      resultBuilder.addAllWarnings(warnings);
    }
    return resultBuilder;
  }

  protected CompletionStage<Response> executeTracingQueryIfNeeded(
      ResponseAndTraceId responseAndTraceId) {
    Response.Builder responseBuilder = responseAndTraceId.responseBuilder;
    return responseAndTraceId.tracingIdIsEmpty()
        ? CompletableFuture.completedFuture(responseBuilder.build())
        : new QueryTracingFetcher(responseAndTraceId.tracingId, connection, getTracingConsistency())
            .fetch()
            .handle(
                (traces, error) -> {
                  if (error == null) {
                    responseBuilder.setTraces(
                        TraceEventsMapper.toTraceEvents(
                            traces, responseBuilder.getTraces().getId()));
                  }
                  // If error != null, ignore and still return the main result with an empty trace
                  // TODO log error?
                  return responseBuilder.build();
                });
  }

  protected void setSuccess(Response response) {
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  protected void handleException(Throwable throwable) {
    if (throwable instanceof CompletionException
        || throwable instanceof ExceptionWithIdempotencyInfo) {
      handleException(throwable.getCause());
    } else if (throwable instanceof StatusException
        || throwable instanceof StatusRuntimeException) {
      responseObserver.onError(throwable);
    } else if (throwable instanceof UnhandledClientException) {
      onError(Status.UNAVAILABLE, throwable);
    } else if (throwable instanceof PersistenceException) {
      handlePersistenceException((PersistenceException) throwable);
    } else {
      LOG.error("Unhandled error returning UNKNOWN to the client", throwable);
      responseObserver.onError(
          Status.UNKNOWN
              .withDescription(throwable.getMessage())
              .withCause(throwable)
              .asRuntimeException());
    }
  }

  private void handlePersistenceException(PersistenceException pe) {
    switch (pe.code()) {
      case SERVER_ERROR:
      case PROTOCOL_ERROR: // Fallthrough
      case UNPREPARED: // Fallthrough
        onError(Status.INTERNAL, pe);
        break;
      case INVALID:
      case SYNTAX_ERROR: // Fallthrough
        onError(Status.INVALID_ARGUMENT, pe);
        break;
      case TRUNCATE_ERROR:
      case CDC_WRITE_FAILURE: // Fallthrough
        onError(Status.ABORTED, pe);
        break;
      case BAD_CREDENTIALS:
        onError(Status.UNAUTHENTICATED, pe);
        break;
      case UNAVAILABLE:
        handleUnavailable((UnavailableException) pe);
        break;
      case OVERLOADED:
        onError(Status.RESOURCE_EXHAUSTED, pe);
        break;
      case IS_BOOTSTRAPPING:
        onError(Status.UNAVAILABLE, pe);
        break;
      case WRITE_TIMEOUT:
        handleWriteTimeout((WriteTimeoutException) pe);
        break;
      case READ_TIMEOUT:
        handleReadTimeout((ReadTimeoutException) pe);
        break;
      case READ_FAILURE:
        handleReadFailure((ReadFailureException) pe);
        break;
      case FUNCTION_FAILURE:
        handleFunctionExecutionException((FunctionExecutionException) pe);
        break;
      case WRITE_FAILURE:
        handleWriteFailure((WriteFailureException) pe);
        break;
      case CAS_WRITE_UNKNOWN:
        handleCasWriteUnknown((CasWriteUnknownResultException) pe);
        break;
      case UNAUTHORIZED:
        onError(Status.PERMISSION_DENIED, pe);
        break;
      case CONFIG_ERROR:
        onError(Status.FAILED_PRECONDITION, pe);
        break;
      case ALREADY_EXISTS:
        handleAlreadyExists((AlreadyExistsException) pe);
        break;
      default:
        LOG.error("Unhandled persistence exception returning UNKNOWN to the client", pe);
        onError(Status.UNKNOWN, pe);
        break;
    }
  }

  private void handleUnavailable(UnavailableException ue) {
    onError(
        Status.UNAVAILABLE,
        ue,
        makeTrailer(
            UNAVAILABLE_KEY,
            Unavailable.newBuilder()
                .setConsistencyValue(ue.consistency.code)
                .setAlive(ue.alive)
                .setRequired(ue.required)
                .build()));
  }

  private void handleWriteTimeout(WriteTimeoutException wte) {
    onError(
        Status.DEADLINE_EXCEEDED,
        wte,
        makeTrailer(
            WRITE_TIMEOUT_KEY,
            WriteTimeout.newBuilder()
                .setConsistencyValue(wte.consistency.code)
                .setBlockFor(wte.blockFor)
                .setReceived(wte.received)
                .setWriteType(wte.writeType.name())
                .build()));
  }

  private void handleReadTimeout(ReadTimeoutException rte) {
    onError(
        Status.DEADLINE_EXCEEDED,
        rte,
        makeTrailer(
            READ_TIMEOUT_KEY,
            ReadTimeout.newBuilder()
                .setConsistencyValue(rte.consistency.code)
                .setBlockFor(rte.blockFor)
                .setReceived(rte.received)
                .setDataPresent(rte.dataPresent)
                .build()));
  }

  private void handleReadFailure(ReadFailureException rfe) {
    onError(
        Status.ABORTED,
        rfe,
        makeTrailer(
            READ_FAILURE_KEY,
            ReadFailure.newBuilder()
                .setConsistencyValue(rfe.consistency.code)
                .setNumFailures(rfe.failureReasonByEndpoint.size())
                .setBlockFor(rfe.blockFor)
                .setReceived(rfe.received)
                .setDataPresent(rfe.dataPresent)
                .build()));
  }

  private void handleFunctionExecutionException(FunctionExecutionException fee) {
    onError(
        Status.FAILED_PRECONDITION,
        fee,
        makeTrailer(
            FUNCTION_FAILURE_KEY,
            FunctionFailure.newBuilder()
                .setKeyspace(fee.functionName.keyspace)
                .setFunction(fee.functionName.name)
                .addAllArgTypes(fee.argTypes)
                .build()));
  }

  private void handleWriteFailure(WriteFailureException wfe) {
    onError(
        Status.ABORTED,
        wfe,
        makeTrailer(
            WRITE_FAILURE_KEY,
            WriteFailure.newBuilder()
                .setConsistencyValue(wfe.consistency.code)
                .setNumFailures(wfe.failureReasonByEndpoint.size())
                .setBlockFor(wfe.blockFor)
                .setReceived(wfe.received)
                .setWriteType(wfe.writeType.name())
                .build()));
  }

  private void handleCasWriteUnknown(CasWriteUnknownResultException cwe) {
    onError(
        Status.ABORTED,
        cwe,
        makeTrailer(
            CAS_WRITE_UNKNOWN_KEY,
            CasWriteUnknown.newBuilder()
                .setConsistencyValue(cwe.consistency.code)
                .setBlockFor(cwe.blockFor)
                .setReceived(cwe.received)
                .build()));
  }

  private void handleAlreadyExists(AlreadyExistsException aee) {
    onError(
        Status.ALREADY_EXISTS,
        aee,
        makeTrailer(
            ALREADY_EXISTS_KEY,
            AlreadyExists.newBuilder().setKeyspace(aee.ksName).setTable(aee.cfName).build()));
  }

  private void onError(Status status, Throwable throwable, Metadata trailer) {
    status = status.withDescription(throwable.getMessage()).withCause(throwable);
    responseObserver.onError(
        trailer != null ? status.asRuntimeException(trailer) : status.asRuntimeException());
  }

  private void onError(Status status, Throwable throwable) {
    onError(status, throwable, null);
  }

  private <T> Metadata makeTrailer(Metadata.Key<T> key, T value) {
    Metadata trailer = new Metadata();
    trailer.put(key, value);
    return trailer;
  }

  protected <V> CompletionStage<V> failedFuture(Exception e, boolean isIdempotent) {
    CompletableFuture<V> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(new ExceptionWithIdempotencyInfo(e, isIdempotent));
    return failedFuture;
  }

  public static class ExceptionWithIdempotencyInfo extends Exception {

    private final boolean isIdempotent;

    public ExceptionWithIdempotencyInfo(Exception e, boolean isIdempotent) {
      super(e);
      this.isIdempotent = isIdempotent;
    }

    public boolean isIdempotent() {
      return isIdempotent;
    }
  }
}
