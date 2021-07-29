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
import com.google.protobuf.Any;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.stargate.db.AuthenticatedUser;
import io.stargate.db.BatchType;
import io.stargate.db.ClientInfo;
import io.stargate.db.ImmutableParameters;
import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.Persistence.Connection;
import io.stargate.db.Result;
import io.stargate.db.Result.Prepared;
import io.stargate.db.Statement;
import io.stargate.grpc.payload.PayloadHandler;
import io.stargate.grpc.payload.PayloadHandlers;
import io.stargate.grpc.service.Service.PrepareInfo;
import io.stargate.grpc.service.Service.ResponseAndTraceId;
import io.stargate.proto.QueryOuterClass.Batch;
import io.stargate.proto.QueryOuterClass.BatchParameters;
import io.stargate.proto.QueryOuterClass.BatchQuery;
import io.stargate.proto.QueryOuterClass.Payload;
import io.stargate.proto.QueryOuterClass.Response;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

class BatchHandler extends MessageHandler {

  /** The maximum number of batch queries to prepare simultaneously. */
  private static final int MAX_CONCURRENT_PREPARES_FOR_BATCH =
      Math.max(Integer.getInteger("stargate.grpc.max_concurrent_prepares_for_batch", 1), 1);

  private final Batch batch;

  BatchHandler(
      Batch batch,
      Connection connection,
      Cache<PrepareInfo, CompletionStage<Prepared>> preparedCache,
      Persistence persistence,
      StreamObserver<Response> responseObserver) {
    super(connection, preparedCache, persistence, responseObserver);
    this.batch = batch;
  }

  void handle() {
    CompletionStage<Result> resultFuture =
        CompletableFuture.<Void>completedFuture(null)
            .thenApply(this::validateQueries)
            .thenCompose(__ -> new BatchPreparer().prepare())
            .thenCompose(this::executePrepared);
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

  private Void validateQueries(Void ignored) {
    if (batch.getQueriesCount() == 0) {
      throw new CompletionException(
          Status.INVALID_ARGUMENT.withDescription("No queries in batch").asException());
    }

    Payload.Type type = batch.getQueries(0).getValues().getType();
    boolean allTypesMatch =
        batch.getQueriesList().stream().allMatch(v -> v.getValues().getType().equals(type));
    if (!allTypesMatch) {
      throw new CompletionException(
          Status.INVALID_ARGUMENT
              .withDescription(
                  "Types for all queries within batch must be the same, and equal to: " + type)
              .asException());
    }
    return null;
  }

  private CompletionStage<Result> executePrepared(io.stargate.db.Batch preparedBatch) {
    long queryStartNanoTime = System.nanoTime();
    BatchParameters parameters = batch.getParameters();
    try {
      return connection.batch(
          preparedBatch, makeParameters(parameters, connection.clientInfo()), queryStartNanoTime);
    } catch (Exception e) {
      return failedFuture(e);
    }
  }

  private CompletionStage<Result> reprepareAndRetry() {
    return new BatchPreparer().prepareForRetry().thenCompose(this::executePrepared);
  }

  private ResponseAndTraceId buildResponse(Result result) {
    ResponseAndTraceId responseAndTraceId = new ResponseAndTraceId();
    responseAndTraceId.setTracingId(result.getTracingId());
    Response.Builder responseBuilder = makeResponseBuilder(result);

    if (result.kind != Result.Kind.Void && result.kind != Result.Kind.Rows) {
      throw new CompletionException(
          Status.INTERNAL.withDescription("Unhandled result kind").asException());
    }

    if (result.kind == Result.Kind.Rows) {
      // all queries within a batch must have the same type
      Payload.Type type = batch.getQueries(0).getValues().getType();
      PayloadHandler handler = PayloadHandlers.get(type);
      try {
        Any data = handler.processResult((Result.Rows) result, batch.getParameters());
        responseBuilder.setResultSet(Payload.newBuilder().setType(type).setData(data));
      } catch (Exception e) {
        throw new CompletionException(e);
      }
    }

    responseAndTraceId.setResponseBuilder(responseBuilder);
    return responseAndTraceId;
  }

  private Parameters makeParameters(BatchParameters parameters, Optional<ClientInfo> clientInfo) {
    ImmutableParameters.Builder builder = ImmutableParameters.builder();

    if (parameters.hasConsistency()) {
      builder.consistencyLevel(
          ConsistencyLevel.fromCode(parameters.getConsistency().getValue().getNumber()));
    }

    if (parameters.hasKeyspace()) {
      builder.defaultKeyspace(parameters.getKeyspace().getValue());
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

    clientInfo.ifPresent(
        c -> {
          Map<String, ByteBuffer> customPayload = new HashMap<>();
          c.storeAuthenticationData(customPayload);
          builder.customPayload(customPayload);
        });

    return builder.tracingRequested(parameters.getTracing()).build();
  }

  @Override
  protected ConsistencyLevel getTracingConsistency() {
    BatchParameters parameters = batch.getParameters();
    return parameters.hasTracingConsistency()
        ? ConsistencyLevel.fromCode(parameters.getTracingConsistency().getValue().getNumber())
        : MessageHandler.DEFAULT_TRACING_CONSISTENCY;
  }

  /**
   * Concurrently prepares queries in a batch. It'll prepare up to {@link
   * #MAX_CONCURRENT_PREPARES_FOR_BATCH} queries simultaneously.
   */
  class BatchPreparer {

    private final AtomicInteger queryIndex = new AtomicInteger();
    private final List<Statement> statements = new CopyOnWriteArrayList<>();
    private final CompletableFuture<io.stargate.db.Batch> future = new CompletableFuture<>();

    /**
     * Initiates the initial prepares. When these prepares finish they'll pull the next available
     * query in the batch and prepare it.
     *
     * @return An future which completes with an internal batch statement with all queries prepared.
     */
    CompletionStage<io.stargate.db.Batch> prepare() {
      return prepare(false);
    }

    CompletionStage<io.stargate.db.Batch> prepareForRetry() {
      return prepare(true);
    }

    private CompletionStage<io.stargate.db.Batch> prepare(boolean shouldInvalidate) {
      int numToPrepare = Math.min(batch.getQueriesCount(), MAX_CONCURRENT_PREPARES_FOR_BATCH);
      assert numToPrepare != 0;
      for (int i = 0; i < numToPrepare; ++i) {
        next(shouldInvalidate);
      }
      return future;
    }

    /** Asynchronously prepares the next query in the batch. */
    private void next(boolean shouldInvalidate) {
      int index = this.queryIndex.getAndIncrement();
      // When there are no more queries to prepare then construct the batch with the prepared
      // statements and complete the future.
      if (index >= batch.getQueriesCount()) {
        future.complete(
            new io.stargate.db.Batch(BatchType.fromId(batch.getTypeValue()), statements));
        return;
      }

      BatchQuery query = batch.getQueries(index);
      BatchParameters batchParameters = batch.getParameters();

      PrepareInfo prepareInfo =
          ImmutablePrepareInfo.builder()
              .keyspace(
                  batchParameters.hasKeyspace() ? batchParameters.getKeyspace().getValue() : null)
              .user(connection.loggedUser().map(AuthenticatedUser::name).orElse(null))
              .cql(query.getCql())
              .build();

      BatchHandler.this
          .prepare(prepareInfo, shouldInvalidate)
          .whenComplete(
              (prepared, t) -> {
                if (t != null) {
                  future.completeExceptionally(t);
                } else {
                  try {
                    PayloadHandler handler = PayloadHandlers.get(query.getValues().getType());
                    statements.add(bindValues(handler, prepared, query.getValues()));
                    next(shouldInvalidate); // Prepare the next query in the batch
                  } catch (Throwable th) {
                    future.completeExceptionally(th);
                  }
                }
              });
    }
  }
}
