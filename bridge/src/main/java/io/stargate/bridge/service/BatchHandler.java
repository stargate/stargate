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

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.QueryOuterClass.Batch;
import io.stargate.bridge.proto.QueryOuterClass.BatchParameters;
import io.stargate.bridge.proto.QueryOuterClass.BatchQuery;
import io.stargate.bridge.proto.QueryOuterClass.Response;
import io.stargate.db.BatchType;
import io.stargate.db.ClientInfo;
import io.stargate.db.ImmutableParameters;
import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.Persistence.Connection;
import io.stargate.db.Result;
import io.stargate.db.Statement;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

public class BatchHandler extends MessageHandler<Batch, BatchHandler.BatchAndIdempotencyInfo> {

  /** The maximum number of batch queries to prepare simultaneously. */
  private static final int MAX_CONCURRENT_PREPARES_FOR_BATCH =
      Math.max(Integer.getInteger("stargate.grpc.max_concurrent_prepares_for_batch", 1), 1);

  private final String decoratedKeyspace;

  BatchHandler(
      Batch batch,
      Connection connection,
      Persistence persistence,
      StreamObserver<Response> responseObserver) {
    super(batch, connection, persistence, responseObserver);
    BatchParameters batchParameters = batch.getParameters();
    this.decoratedKeyspace =
        batchParameters.hasKeyspace()
            ? persistence.decorateKeyspaceName(
                batchParameters.getKeyspace().getValue(), BridgeService.HEADERS_KEY.get())
            : null;
  }

  @Override
  protected void validate() throws Exception {
    if (message.getQueriesCount() == 0) {
      throw Status.INVALID_ARGUMENT.withDescription("No queries in batch").asException();
    }
  }

  @Override
  protected CompletionStage<BatchAndIdempotencyInfo> prepare() {
    return new BatchPreparer().prepare();
  }

  @Override
  protected CompletionStage<Result> executePrepared(BatchAndIdempotencyInfo preparedBatch) {
    long queryStartNanoTime = System.nanoTime();
    BatchParameters parameters = message.getParameters();
    try {
      return connection.batch(
          preparedBatch.batch,
          makeParameters(parameters, connection.clientInfo()),
          queryStartNanoTime);
    } catch (Exception e) {
      return failedFuture(e, preparedBatch.isIdempotent);
    }
  }

  @Override
  protected CompletionStage<BridgeService.ResponseAndTraceId> buildResponse(Result result) {
    Response.Builder responseBuilder = makeResponseBuilder(result);

    if (result.kind != Result.Kind.Void && result.kind != Result.Kind.Rows) {
      throw new CompletionException(
          Status.INTERNAL.withDescription("Unhandled result kind").asException());
    }

    if (result.kind == Result.Kind.Rows) {
      // all queries within a batch must have the same type
      try {
        responseBuilder.setResultSet(
            ValuesHelper.processResult((Result.Rows) result, message.getParameters()));
      } catch (Exception e) {
        throw new CompletionException(e);
      }
    }

    return CompletableFuture.completedFuture(
        BridgeService.ResponseAndTraceId.from(result, responseBuilder));
  }

  @Override
  protected ConsistencyLevel getTracingConsistency() {
    BatchParameters parameters = message.getParameters();
    return parameters.hasTracingConsistency()
        ? ConsistencyLevel.fromCode(parameters.getTracingConsistency().getValue().getNumber())
        : MessageHandler.DEFAULT_TRACING_CONSISTENCY;
  }

  @Override
  protected void setSuccess(QueryOuterClass.Response response) {
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  private Parameters makeParameters(BatchParameters parameters, Optional<ClientInfo> clientInfo) {
    ImmutableParameters.Builder builder = ImmutableParameters.builder();

    builder.consistencyLevel(
        parameters.hasConsistency()
            ? ConsistencyLevel.fromCode(parameters.getConsistency().getValue().getNumber())
            : BridgeService.DEFAULT_CONSISTENCY);

    if (decoratedKeyspace != null) {
      builder.defaultKeyspace(decoratedKeyspace);
    }

    builder.serialConsistencyLevel(
        parameters.hasSerialConsistency()
            ? ConsistencyLevel.fromCode(parameters.getSerialConsistency().getValue().getNumber())
            : BridgeService.DEFAULT_SERIAL_CONSISTENCY);

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

  /**
   * Concurrently prepares queries in a batch. It'll prepare up to {@link
   * #MAX_CONCURRENT_PREPARES_FOR_BATCH} queries simultaneously.
   */
  class BatchPreparer {

    private final AtomicInteger queryIndex = new AtomicInteger();
    private final List<Statement> statements = new CopyOnWriteArrayList<>();
    private final CompletableFuture<BatchAndIdempotencyInfo> future = new CompletableFuture<>();
    private final AtomicBoolean isIdempotent = new AtomicBoolean(true);

    /**
     * Initiates the initial prepares. When these prepares finish they'll pull the next available
     * query in the batch and prepare it.
     *
     * @return A future which completes with an internal batch statement with all queries prepared.
     */
    CompletionStage<BatchAndIdempotencyInfo> prepare() {
      int numToPrepare = Math.min(message.getQueriesCount(), MAX_CONCURRENT_PREPARES_FOR_BATCH);
      assert numToPrepare != 0;
      for (int i = 0; i < numToPrepare; ++i) {
        next();
      }
      return future;
    }

    /** Asynchronously prepares the next query in the batch. */
    private void next() {
      int index = this.queryIndex.getAndIncrement();
      // When there are no more queries to prepare then construct the batch with the prepared
      // statements and complete the future.
      if (index >= message.getQueriesCount()) {
        future.complete(
            new BatchAndIdempotencyInfo(
                new io.stargate.db.Batch(BatchType.fromId(message.getTypeValue()), statements),
                isIdempotent.get()));
        return;
      }

      BatchQuery query = message.getQueries(index);

      BatchHandler.this
          .prepare(query.getCql(), decoratedKeyspace)
          .whenComplete(
              (prepared, t) -> {
                if (t != null) {
                  future.completeExceptionally(t);
                } else {
                  try {
                    // if any statement in a batch is non idempotent, then all statements are non
                    // idempotent
                    isIdempotent.compareAndSet(true, prepared.isIdempotent);
                    statements.add(bindValues(prepared, query.getValues()));
                    next(); // Prepare the next query in the batch
                  } catch (Throwable th) {
                    future.completeExceptionally(th);
                  }
                }
              });
    }
  }

  static class BatchAndIdempotencyInfo {
    public final io.stargate.db.Batch batch;
    public final boolean isIdempotent;

    BatchAndIdempotencyInfo(io.stargate.db.Batch batch, boolean isIdempotent) {
      this.batch = batch;
      this.isIdempotent = isIdempotent;
    }
  }
}
