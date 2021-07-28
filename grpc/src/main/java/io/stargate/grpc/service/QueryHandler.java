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
import com.google.common.base.Supplier;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.stargate.db.AuthenticatedUser;
import io.stargate.db.BoundStatement;
import io.stargate.db.ImmutableParameters;
import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.Persistence.Connection;
import io.stargate.db.Result;
import io.stargate.db.Result.Prepared;
import io.stargate.db.tracing.QueryTracingFetcher;
import io.stargate.grpc.payload.PayloadHandler;
import io.stargate.grpc.payload.PayloadHandlers;
import io.stargate.grpc.service.Service.PrepareInfo;
import io.stargate.grpc.service.Service.ResponseAndTraceId;
import io.stargate.grpc.tracing.TraceEventsMapper;
import io.stargate.proto.QueryOuterClass.Payload;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.QueryParameters;
import io.stargate.proto.QueryOuterClass.Response;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.apache.cassandra.stargate.exceptions.PreparedQueryNotFoundException;

class QueryHandler extends MessageHandler {

  private final Query query;
  private final PrepareInfo prepareInfo;

  QueryHandler(
      Query query,
      Connection connection,
      Cache<PrepareInfo, CompletionStage<Prepared>> preparedCache,
      Persistence persistence,
      StreamObserver<Response> responseObserver) {
    super(connection, preparedCache, persistence, responseObserver);
    this.query = query;
    QueryParameters queryParameters = query.getParameters();
    this.prepareInfo =
        ImmutablePrepareInfo.builder()
            .keyspace(
                queryParameters.hasKeyspace() ? queryParameters.getKeyspace().getValue() : null)
            .user(connection.loggedUser().map(AuthenticatedUser::name).orElse(null))
            .cql(query.getCql())
            .build();
  }

  void handle() {
    CompletionStage<Result> resultFuture = prepare(prepareInfo).thenCompose(this::executePrepared);

    // If our local cache got out of sync with the server, we might get an UNPREPARED response,
    // reprepare on the fly and retry:
    resultFuture = handleUnprepared(resultFuture, this::reprepareAndRetry);

    resultFuture
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

  private CompletionStage<Result> executePrepared(Prepared prepared) {
    long queryStartNanoTime = System.nanoTime();

    Payload values = query.getValues();
    PayloadHandler handler = PayloadHandlers.get(values.getType());

    QueryParameters parameters = query.getParameters();

    try {
      return connection.execute(
          bindValues(handler, prepared, values), makeParameters(parameters), queryStartNanoTime);
    } catch (Exception e) {
      CompletableFuture<Result> failedFuture = new CompletableFuture<>();
      failedFuture.completeExceptionally(e);
      return failedFuture;
    }
  }

  private CompletionStage<Result> reprepareAndRetry() {
    return prepare(prepareInfo, true).thenCompose(this::executePrepared);
  }

  private ResponseAndTraceId buildResponse(Result result) {
    ResponseAndTraceId responseAndTraceId = new ResponseAndTraceId();
    responseAndTraceId.setTracingId(result.getTracingId());
    Response.Builder responseBuilder = makeResponseBuilder(result);
    switch (result.kind) {
      case Void:
        break;
      case SchemaChange:
        // TODO make this non-blocking?
        persistence.waitForSchemaAgreement();
        break;
      case Rows:
        Payload values = query.getValues();
        PayloadHandler handler = PayloadHandlers.get(values.getType());
        try {
          responseBuilder.setResultSet(
              Payload.newBuilder()
                  .setType(query.getValues().getType())
                  .setData(handler.processResult((Result.Rows) result, query.getParameters())));
        } catch (Exception e) {
          throw new CompletionException(e);
        }
        break;
      case SetKeyspace:
        throw new CompletionException(
            Status.INVALID_ARGUMENT.withDescription("USE <keyspace> not supported").asException());
      default:
        throw new CompletionException(
            Status.INTERNAL.withDescription("Unhandled result kind").asException());
    }
    responseAndTraceId.setResponseBuilder(responseBuilder);
    return responseAndTraceId;
  }

  private CompletionStage<Response> executeTracingQueryIfNeeded(
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

  private BoundStatement bindValues(PayloadHandler handler, Prepared prepared, Payload values)
      throws Exception {
    if (!values.hasData()) {
      return new BoundStatement(prepared.statementId, Collections.emptyList(), null);
    }
    return handler.bindValues(prepared, values.getData(), persistence.unsetValue());
  }

  private Parameters makeParameters(QueryParameters parameters) {
    ImmutableParameters.Builder builder = ImmutableParameters.builder();

    if (parameters.hasConsistency()) {
      builder.consistencyLevel(
          ConsistencyLevel.fromCode(parameters.getConsistency().getValue().getNumber()));
    }

    if (parameters.hasKeyspace()) {
      builder.defaultKeyspace(parameters.getKeyspace().getValue());
    }

    if (parameters.hasPageSize()) {
      builder.pageSize(parameters.getPageSize().getValue());
    }

    if (parameters.hasPagingState()) {
      builder.pagingState(ByteBuffer.wrap(parameters.getPagingState().getValue().toByteArray()));
    }

    if (parameters.hasSerialConsistency()) {
      builder.serialConsistencyLevel(
          ConsistencyLevel.fromCode(parameters.getSerialConsistency().getValue().getNumber()));
    }

    if (parameters.hasTimestamp()) {
      builder.defaultTimestamp(parameters.getTimestamp().getValue());
    }

    if (parameters.hasNowInSeconds()) {
      builder.nowInSeconds(parameters.getNowInSeconds().getValue());
    }

    return builder.tracingRequested(parameters.getTracing()).build();
  }

  private Response.Builder makeResponseBuilder(io.stargate.db.Result result) {
    Response.Builder resultBuilder = Response.newBuilder();
    List<String> warnings = result.getWarnings();
    if (warnings != null) {
      resultBuilder.addAllWarnings(warnings);
    }
    return resultBuilder;
  }

  private ConsistencyLevel getTracingConsistency() {
    QueryParameters parameters = query.getParameters();
    return parameters.hasTracingConsistency()
        ? ConsistencyLevel.fromCode(parameters.getTracingConsistency().getValue().getNumber())
        : Service.DEFAULT_TRACING_CONSISTENCY;
  }

  /**
   * If the source future is failed with {@link PreparedQueryNotFoundException}, return the result
   * of {@code onUnprepared}, otherwise return an equivalent of the source future.
   */
  private CompletionStage<Result> handleUnprepared(
      CompletionStage<Result> source, Supplier<CompletionStage<Result>> onUnprepared) {
    CompletableFuture<Result> target = new CompletableFuture<>();
    source.whenComplete(
        (result, error) -> {
          if (error != null) {
            if (error instanceof PreparedQueryNotFoundException) {
              onUnprepared
                  .get()
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
}
