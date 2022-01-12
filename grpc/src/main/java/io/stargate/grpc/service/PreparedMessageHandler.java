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

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Status;
import io.stargate.db.BoundStatement;
import io.stargate.db.ImmutableParameters;
import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.Persistence.Connection;
import io.stargate.db.Result;
import io.stargate.db.Result.Prepared;
import io.stargate.db.tracing.QueryTracingFetcher;
import io.stargate.grpc.service.GrpcService.ResponseAndTraceId;
import io.stargate.grpc.tracing.TraceEventsMapper;
import io.stargate.proto.QueryOuterClass.Response;
import io.stargate.proto.QueryOuterClass.Values;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import javax.annotation.Nullable;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

/**
 * @param <MessageT> the type of gRPC message being handled.
 * @param <PreparedT> the persistence object resulting from the preparation of the query(ies).
 */
abstract class PreparedMessageHandler<MessageT extends GeneratedMessageV3, PreparedT>
    extends MessageHandler<MessageT> {

  protected final Connection connection;
  protected final Persistence persistence;

  protected PreparedMessageHandler(
      MessageT message,
      Connection connection,
      Persistence persistence,
      ExceptionHandler exceptionHandler) {
    super(message, exceptionHandler);
    this.connection = connection;
    this.persistence = persistence;
  }

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

  @Override
  protected CompletionStage<Response> executeQuery() {
    CompletionStage<Result> resultFuture = prepare().thenCompose(this::executePrepared);
    return handleUnprepared(resultFuture)
        .thenCompose(this::buildResponse)
        .thenCompose(this::executeTracingQueryIfNeeded);
  }

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
}
