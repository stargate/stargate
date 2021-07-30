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

import com.github.benmanes.caffeine.cache.Cache;
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
import io.stargate.grpc.payload.PayloadHandler;
import io.stargate.grpc.service.Service.PrepareInfo;
import io.stargate.grpc.service.Service.ResponseAndTraceId;
import io.stargate.grpc.tracing.TraceEventsMapper;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.QueryOuterClass.Response;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.apache.cassandra.stargate.exceptions.AlreadyExistsException;
import org.apache.cassandra.stargate.exceptions.CasWriteUnknownResultException;
import org.apache.cassandra.stargate.exceptions.FunctionExecutionException;
import org.apache.cassandra.stargate.exceptions.PersistenceException;
import org.apache.cassandra.stargate.exceptions.PreparedQueryNotFoundException;
import org.apache.cassandra.stargate.exceptions.ReadFailureException;
import org.apache.cassandra.stargate.exceptions.ReadTimeoutException;
import org.apache.cassandra.stargate.exceptions.UnavailableException;
import org.apache.cassandra.stargate.exceptions.WriteFailureException;
import org.apache.cassandra.stargate.exceptions.WriteTimeoutException;

/**
 * @param <MessageT> the type of gRPC message being handled.
 * @param <PreparedT> the persistence object resulting from the preparation of the query(ies).
 */
abstract class MessageHandler<MessageT extends GeneratedMessageV3, PreparedT> {

  static final Metadata.Key<QueryOuterClass.Unavailable> UNAVAILABLE_KEY =
      ProtoUtils.keyForProto(QueryOuterClass.Unavailable.getDefaultInstance());
  static final Metadata.Key<QueryOuterClass.WriteTimeout> WRITE_TIMEOUT_KEY =
      ProtoUtils.keyForProto(QueryOuterClass.WriteTimeout.getDefaultInstance());
  static final Metadata.Key<QueryOuterClass.ReadTimeout> READ_TIMEOUT_KEY =
      ProtoUtils.keyForProto(QueryOuterClass.ReadTimeout.getDefaultInstance());
  static final Metadata.Key<QueryOuterClass.ReadFailure> READ_FAILURE_KEY =
      ProtoUtils.keyForProto(QueryOuterClass.ReadFailure.getDefaultInstance());
  static final Metadata.Key<QueryOuterClass.FunctionFailure> FUNCTION_FAILURE_KEY =
      ProtoUtils.keyForProto(QueryOuterClass.FunctionFailure.getDefaultInstance());
  static final Metadata.Key<QueryOuterClass.WriteFailure> WRITE_FAILURE_KEY =
      ProtoUtils.keyForProto(QueryOuterClass.WriteFailure.getDefaultInstance());
  static final Metadata.Key<QueryOuterClass.AlreadyExists> ALREADY_EXISTS_KEY =
      ProtoUtils.keyForProto(QueryOuterClass.AlreadyExists.getDefaultInstance());
  static final Metadata.Key<QueryOuterClass.CasWriteUnknown> CAS_WRITE_UNKNOWN_KEY =
      ProtoUtils.keyForProto(QueryOuterClass.CasWriteUnknown.getDefaultInstance());

  protected static final ConsistencyLevel DEFAULT_TRACING_CONSISTENCY = ConsistencyLevel.ONE;

  protected final MessageT message;
  protected final Connection connection;
  private final Cache<PrepareInfo, CompletionStage<Prepared>> preparedCache;
  protected final Persistence persistence;
  private final StreamObserver<Response> responseObserver;

  protected MessageHandler(
      MessageT message,
      Connection connection,
      Cache<PrepareInfo, CompletionStage<Prepared>> preparedCache,
      Persistence persistence,
      StreamObserver<Response> responseObserver) {
    this.message = message;
    this.connection = connection;
    this.preparedCache = preparedCache;
    this.persistence = persistence;
    this.responseObserver = responseObserver;
  }

  void handle() {
    CompletionStage<Result> resultFuture =
        CompletableFuture.<Void>completedFuture(null)
            .thenApply(this::invokeValidate)
            .thenCompose(__ -> prepare(false))
            .thenCompose(this::executePrepared);
    handleUnprepared(resultFuture)
        .thenApply(this::buildResponse)
        .thenCompose(this::executeTracingQueryIfNeeded)
        .whenComplete(
            (response, error) -> {
              if (error != null) {
                handleException(error);
              } else {
                setSuccess(response);
              }
            });
  }

  /** Performs any necessary validation on the message before execution starts. */
  protected abstract void validate() throws Exception;

  /**
   * Prepares any CQL query required for the execution of the request, and returns an executable
   * object.
   *
   * @param shouldInvalidate whether to invalidate the corresponding entries in the prepared
   *     statement cache.
   */
  protected abstract CompletionStage<PreparedT> prepare(boolean shouldInvalidate);

  /** Executes the prepared object to get the CQL results. */
  protected abstract CompletionStage<Result> executePrepared(PreparedT prepared);

  /** Builds the gRPC response from the CQL result. */
  protected abstract ResponseAndTraceId buildResponse(Result result);

  /** Computes the consistency level to use for tracing queries. */
  protected abstract ConsistencyLevel getTracingConsistency();

  private Void invokeValidate(Void ignored) {
    try {
      validate();
      return null;
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new CompletionException(e);
    }
  }

  protected BoundStatement bindValues(
      PayloadHandler handler, Prepared prepared, QueryOuterClass.Payload values) throws Exception {
    return values.hasData()
        ? handler.bindValues(prepared, values.getData(), persistence.unsetValue())
        : new BoundStatement(prepared.statementId, Collections.emptyList(), null);
  }

  protected CompletionStage<Prepared> prepare(PrepareInfo prepareInfo, boolean shouldInvalidate) {
    // In the event a query is being retried due to a PreparedQueryNotFoundException invalidate the
    // local cache to refresh with the remote cache
    if (shouldInvalidate) {
      preparedCache.invalidate(prepareInfo);
    }
    CompletionStage<Prepared> result = preparedCache.getIfPresent(prepareInfo);
    if (result == null) {
      // Cache miss: compute the entry ourselves, but be prepared for another thread trying
      // concurrently
      CompletableFuture<Prepared> myEntry = new CompletableFuture<>();
      result = preparedCache.get(prepareInfo, __ -> myEntry);
      if (result == myEntry) {
        prepareOnServer(prepareInfo)
            .whenComplete(
                (prepared, error) -> {
                  if (error != null) {
                    myEntry.completeExceptionally(error);
                    // Don't cache failures:
                    preparedCache.invalidate(prepareInfo);
                  } else {
                    myEntry.complete(prepared);
                  }
                });
      }
    }
    return result;
  }

  private CompletionStage<Prepared> prepareOnServer(PrepareInfo prepareInfo) {
    String keyspace = prepareInfo.keyspace();
    Parameters parameters =
        (keyspace == null)
            ? Parameters.defaults()
            : ImmutableParameters.builder().defaultKeyspace(keyspace).build();
    return connection.prepare(prepareInfo.cql(), parameters);
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
            if (error instanceof PreparedQueryNotFoundException) {
              reprepareAndRetry()
                  .whenComplete(
                      (result2, error2) -> {
                        if (error2 != null) {
                          target.completeExceptionally(error2);
                        } else {
                          target.complete(result2);
                        }
                      });
            } else {
              target.completeExceptionally(error);
            }
          } else {
            target.complete(result);
          }
        });
    return target;
  }

  private CompletionStage<Result> reprepareAndRetry() {
    return prepare(true).thenCompose(this::executePrepared);
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
    return (responseAndTraceId.tracingIdIsEmpty())
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
    if (throwable instanceof CompletionException) {
      handleException(throwable.getCause());
    } else if (throwable instanceof StatusException
        || throwable instanceof StatusRuntimeException) {
      responseObserver.onError(throwable);
    } else if (throwable instanceof PersistenceException) {
      handlePersistenceException((PersistenceException) throwable);
    } else {
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
        UnavailableException ue = (UnavailableException) pe;
        onError(
            Status.UNAVAILABLE,
            ue,
            makeTrailer(
                UNAVAILABLE_KEY,
                QueryOuterClass.Unavailable.newBuilder()
                    .setConsistencyValue(ue.consistency.code)
                    .setAlive(ue.alive)
                    .setRequired(ue.required)
                    .build()));
        break;
      case OVERLOADED:
        onError(Status.RESOURCE_EXHAUSTED, pe);
        break;
      case IS_BOOTSTRAPPING:
        onError(Status.UNAVAILABLE, pe);
        break;
      case WRITE_TIMEOUT:
        WriteTimeoutException wte = (WriteTimeoutException) pe;
        onError(
            Status.DEADLINE_EXCEEDED,
            pe,
            makeTrailer(
                WRITE_TIMEOUT_KEY,
                QueryOuterClass.WriteTimeout.newBuilder()
                    .setConsistencyValue(wte.consistency.code)
                    .setBlockFor(wte.blockFor)
                    .setReceived(wte.received)
                    .setWriteType(wte.writeType.name())
                    .build()));
        break;
      case READ_TIMEOUT:
        ReadTimeoutException rte = (ReadTimeoutException) pe;
        onError(
            Status.DEADLINE_EXCEEDED,
            pe,
            makeTrailer(
                READ_TIMEOUT_KEY,
                QueryOuterClass.ReadTimeout.newBuilder()
                    .setConsistencyValue(rte.consistency.code)
                    .setBlockFor(rte.blockFor)
                    .setReceived(rte.received)
                    .setDataPresent(rte.dataPresent)
                    .build()));
        break;
      case READ_FAILURE:
        ReadFailureException rfe = (ReadFailureException) pe;
        onError(
            Status.ABORTED,
            pe,
            makeTrailer(
                READ_FAILURE_KEY,
                QueryOuterClass.ReadFailure.newBuilder()
                    .setConsistencyValue(rfe.consistency.code)
                    .setNumFailures(rfe.failureReasonByEndpoint.size())
                    .setBlockFor(rfe.blockFor)
                    .setReceived(rfe.received)
                    .setDataPresent(rfe.dataPresent)
                    .build()));
        break;
      case FUNCTION_FAILURE:
        FunctionExecutionException fee = (FunctionExecutionException) pe;
        onError(
            Status.FAILED_PRECONDITION,
            pe,
            makeTrailer(
                FUNCTION_FAILURE_KEY,
                QueryOuterClass.FunctionFailure.newBuilder()
                    .setKeyspace(fee.functionName.keyspace)
                    .setFunction(fee.functionName.name)
                    .addAllArgTypes(fee.argTypes)
                    .build()));
        break;
      case WRITE_FAILURE:
        WriteFailureException wfe = (WriteFailureException) pe;
        onError(
            Status.ABORTED,
            pe,
            makeTrailer(
                WRITE_FAILURE_KEY,
                QueryOuterClass.WriteFailure.newBuilder()
                    .setConsistencyValue(wfe.consistency.code)
                    .setNumFailures(wfe.failureReasonByEndpoint.size())
                    .setBlockFor(wfe.blockFor)
                    .setReceived(wfe.received)
                    .setWriteType(wfe.writeType.name())
                    .build()));
        break;
      case CAS_WRITE_UNKNOWN:
        CasWriteUnknownResultException cwe = (CasWriteUnknownResultException) pe;
        onError(
            Status.ABORTED,
            pe,
            makeTrailer(
                CAS_WRITE_UNKNOWN_KEY,
                QueryOuterClass.CasWriteUnknown.newBuilder()
                    .setConsistencyValue(cwe.consistency.code)
                    .setBlockFor(cwe.blockFor)
                    .setReceived(cwe.received)
                    .build()));
        break;
      case UNAUTHORIZED:
        onError(Status.PERMISSION_DENIED, pe);
        break;
      case CONFIG_ERROR:
        onError(Status.FAILED_PRECONDITION, pe);
        break;
      case ALREADY_EXISTS:
        AlreadyExistsException aee = (AlreadyExistsException) pe;
        onError(
            Status.ALREADY_EXISTS,
            pe,
            makeTrailer(
                ALREADY_EXISTS_KEY,
                QueryOuterClass.AlreadyExists.newBuilder()
                    .setKeyspace(aee.ksName)
                    .setTable(aee.cfName)
                    .build()));
        break;
      default:
        onError(Status.UNKNOWN, pe);
        break;
    }
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

  protected <V> CompletionStage<V> failedFuture(Exception e) {
    CompletableFuture<V> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(e);
    return failedFuture;
  }
}
