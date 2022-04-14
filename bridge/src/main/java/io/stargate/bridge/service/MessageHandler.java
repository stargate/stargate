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

import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.StringValue;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.QueryOuterClass.Response;
import io.stargate.bridge.proto.QueryOuterClass.Values;
import io.stargate.bridge.retries.DefaultRetryPolicy;
import io.stargate.bridge.retries.RetryDecision;
import io.stargate.bridge.tracing.TraceEventsMapper;
import io.stargate.db.BoundStatement;
import io.stargate.db.ImmutableParameters;
import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.Persistence.Connection;
import io.stargate.db.Result;
import io.stargate.db.Result.Prepared;
import io.stargate.db.Result.SchemaChange;
import io.stargate.db.Result.SchemaChangeMetadata;
import io.stargate.db.tracing.QueryTracingFetcher;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import javax.annotation.Nullable;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.apache.cassandra.stargate.exceptions.PersistenceException;
import org.apache.cassandra.stargate.exceptions.PreparedQueryNotFoundException;
import org.apache.cassandra.stargate.exceptions.ReadTimeoutException;
import org.apache.cassandra.stargate.exceptions.WriteTimeoutException;

/**
 * @param <MessageT> the type of gRPC message being handled.
 * @param <PreparedT> the persistence object resulting from the preparation of the query(ies).
 */
public abstract class MessageHandler<MessageT extends GeneratedMessageV3, PreparedT> {

  protected static final ConsistencyLevel DEFAULT_TRACING_CONSISTENCY = ConsistencyLevel.ONE;

  protected final MessageT message;
  protected final Connection connection;
  protected final Persistence persistence;
  private final DefaultRetryPolicy retryPolicy;
  private final ExceptionHandler exceptionHandler;

  protected MessageHandler(
      MessageT message,
      Connection connection,
      Persistence persistence,
      ExceptionHandler exceptionHandler) {
    this.message = message;
    this.connection = connection;
    this.persistence = persistence;
    this.retryPolicy = new DefaultRetryPolicy();
    this.exceptionHandler = exceptionHandler;
  }

  public void handle() {
    try {
      validate();
      executeWithRetry(0);

    } catch (Throwable t) {
      exceptionHandler.handleException(t);
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
                    exceptionHandler.handleException(error);
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
      return RetryDecision.RETHROW;
    }
    PersistenceException pe = cause.get();
    switch (pe.code()) {
      case UNPREPARED:
        return retryPolicy.onUnprepared((PreparedQueryNotFoundException) pe, retryCount);
      case READ_TIMEOUT:
        return retryPolicy.onReadTimeout((ReadTimeoutException) pe, retryCount);
      case WRITE_TIMEOUT:
        if (isIdempotent(throwable)) {
          return retryPolicy.onWriteTimeout((WriteTimeoutException) pe, retryCount);
        } else {
          return RetryDecision.RETHROW;
        }
      default:
        return RetryDecision.RETHROW;
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
  protected abstract CompletionStage<BridgeService.ResponseAndTraceId> buildResponse(Result result);

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
      BridgeService.ResponseAndTraceId responseAndTraceId) {
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

  protected abstract void setSuccess(Response response);

  protected <V> CompletionStage<V> failedFuture(Exception e, boolean isIdempotent) {
    CompletableFuture<V> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(new ExceptionWithIdempotencyInfo(e, isIdempotent));
    return failedFuture;
  }

  protected static QueryOuterClass.SchemaChange buildSchemaChange(SchemaChange result) {
    SchemaChangeMetadata metadata = result.metadata;
    QueryOuterClass.SchemaChange.Builder schemaChangeBuilder =
        QueryOuterClass.SchemaChange.newBuilder()
            .setChangeType(QueryOuterClass.SchemaChange.Type.valueOf(metadata.change))
            .setTarget(QueryOuterClass.SchemaChange.Target.valueOf(metadata.target))
            .setKeyspace(metadata.keyspace);
    if (metadata.name != null) {
      schemaChangeBuilder.setName(StringValue.of(metadata.name));
    }
    if (metadata.argTypes != null) {
      schemaChangeBuilder.addAllArgumentTypes(metadata.argTypes);
    }
    return schemaChangeBuilder.build();
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
