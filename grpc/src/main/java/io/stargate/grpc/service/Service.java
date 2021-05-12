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
import com.github.benmanes.caffeine.cache.Caffeine;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.db.AuthenticatedUser;
import io.stargate.db.BatchType;
import io.stargate.db.BoundStatement;
import io.stargate.db.ClientInfo;
import io.stargate.db.ImmutableParameters;
import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.Persistence.Connection;
import io.stargate.db.Result.Kind;
import io.stargate.db.Result.Prepared;
import io.stargate.db.Result.Rows;
import io.stargate.db.Statement;
import io.stargate.grpc.payload.PayloadHandler;
import io.stargate.grpc.payload.PayloadHandlers;
import io.stargate.proto.QueryOuterClass.Batch;
import io.stargate.proto.QueryOuterClass.BatchParameters;
import io.stargate.proto.QueryOuterClass.BatchQuery;
import io.stargate.proto.QueryOuterClass.Payload;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.QueryParameters;
import io.stargate.proto.QueryOuterClass.Result;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

public class Service extends io.stargate.proto.StargateGrpc.StargateImplBase {

  public static final Context.Key<AuthenticationSubject> AUTHENTICATION_KEY =
      Context.key("authentication");
  public static final Context.Key<SocketAddress> REMOTE_ADDRESS_KEY = Context.key("remoteAddress");

  private static final InetSocketAddress DUMMY_ADDRESS = new InetSocketAddress(9042);

  /** The maximum number of batch queries to prepare simultaneously. */
  private static final int MAX_CONCURRENT_PREPARES_FOR_BATCH =
      Math.max(Integer.getInteger("stargate.grpc.max_concurrent_prepares_for_batch", 1), 1);

  // TODO: Add a maximum size and add tuning options
  private final Cache<String, Prepared> preparedCache = Caffeine.newBuilder().build();

  private final Persistence persistence;
  private final ByteBuffer unsetValue;

  @SuppressWarnings("unused")
  private final Metrics metrics;

  public Service(Persistence persistence, Metrics metrics) {
    this.persistence = persistence;
    this.metrics = metrics;
    assert this.metrics != null;
    unsetValue = persistence.unsetValue();
  }

  @Override
  public void executeQuery(Query query, StreamObserver<Result> responseObserver) {
    try {
      AuthenticationSubject authenticationSubject = AUTHENTICATION_KEY.get();
      Connection connection = newConnection(authenticationSubject.asUser());
      QueryParameters queryParameters = query.getParameters();

      prepareQuery(
              connection,
              query.getCql(),
              queryParameters.hasKeyspace() ? queryParameters.getKeyspace().getValue() : null,
              queryParameters.getTracing())
          .whenComplete(
              (prepared, t) -> {
                if (t != null) {
                  handleError(t, responseObserver);
                } else {
                  executePrepared(connection, prepared, query, responseObserver);
                }
              });
    } catch (Exception e) {
      handleError(e, responseObserver);
    }
  }

  @Override
  public void executeBatch(Batch batch, StreamObserver<Result> responseObserver) {
    try {
      AuthenticationSubject authenticationSubject = AUTHENTICATION_KEY.get();
      Connection connection = newConnection(authenticationSubject.asUser());

      if (batch.getQueriesCount() == 0) {
        responseObserver.onError(
            Status.INVALID_ARGUMENT.withDescription("No queries in batch").asException());
        return;
      }

      // TODO: Add a limit for the maximum number of queries in a batch? The setting
      // `batch_size_fail_threshold_in_kb` provides some protection at the persistence layer.

      new BatchPreparer(connection, batch)
          .prepare()
          .whenComplete(
              (preparedBatch, t) -> {
                if (t != null) {
                  handleError(t, responseObserver);
                } else {
                  executeBatch(connection, preparedBatch, batch.getParameters(), responseObserver);
                }
              });

    } catch (Exception e) {
      handleError(e, responseObserver);
    }
  }

  private void handleError(Throwable throwable, StreamObserver<Result> responseObserver) {
    if (throwable instanceof StatusException || throwable instanceof StatusRuntimeException) {
      responseObserver.onError(throwable);
    } else {
      responseObserver.onError(
          Status.UNKNOWN
              .withDescription(throwable.getMessage())
              .withCause(throwable)
              .asRuntimeException());
    }
  }

  private CompletableFuture<Prepared> prepareQuery(
      Connection connection, String cql, String keyspace, boolean tracing) {
    CompletableFuture<Prepared> future = new CompletableFuture<>();
    final StringBuilder keyBuilder = new StringBuilder();
    connection.loggedUser().ifPresent(user -> keyBuilder.append(user.name()));
    if (keyspace != null) {
      keyBuilder.append(keyspace);
    }
    keyBuilder.append(cql);
    final String key = keyBuilder.toString();
    // Caching here to avoid round trip to the persistence backend thread.
    Prepared prepared = preparedCache.getIfPresent(key);
    if (prepared != null) {
      future.complete(prepared);
    } else {
      ImmutableParameters.Builder parameterBuilder =
          ImmutableParameters.builder().tracingRequested(tracing);
      if (keyspace != null) {
        parameterBuilder.defaultKeyspace(keyspace);
      }
      connection
          .prepare(cql, parameterBuilder.build())
          .whenComplete(
              (p, t) -> {
                if (t != null) {
                  future.completeExceptionally(t);
                } else {
                  preparedCache.put(key, p);
                  future.complete(p);
                }
              });
    }
    return future;
  }

  private void executePrepared(
      Connection connection,
      Prepared prepared,
      Query query,
      StreamObserver<Result> responseObserver) {
    try {
      long queryStartNanoTime = System.nanoTime();

      QueryParameters parameters = query.getParameters();
      Payload payload = parameters.getPayload();

      PayloadHandler handler = PayloadHandlers.get(payload.getType());

      connection
          .execute(
              bindValues(handler, prepared, payload),
              makeParameters(parameters),
              queryStartNanoTime)
          .whenComplete(
              (result, t) -> {
                if (t != null) {
                  handleError(t, responseObserver);
                } else {
                  try {
                    Result.Builder resultBuilder = makeResultBuilder(result);
                    switch (result.kind) {
                      case Void:
                        break;
                      case Rows:
                        Rows rows = (Rows) result;
                        if (rows.rows.isEmpty()
                            && (parameters.getSkipMetadata()
                                || rows.resultMetadata.columns.isEmpty())) {
                          resultBuilder.setPayload(
                              Payload.newBuilder().setType(payload.getType()).build());
                        } else {
                          resultBuilder.setPayload(
                              handler.processResult((Rows) result, parameters));
                        }
                        break;
                      case SchemaChange:
                        // TODO: Wait for schema agreement, etc. Could this be made async? This is
                        // blocking the gRPC thread.
                        persistence.waitForSchemaAgreement();
                        break;
                      case SetKeyspace:
                        // TODO: Prevent "USE <keyspace>" from happening
                        throw Status.INTERNAL
                            .withDescription("USE <keyspace> not supported")
                            .asException();
                      default:
                        throw Status.INTERNAL
                            .withDescription("Unhandled result kind")
                            .asException();
                    }
                    responseObserver.onNext(resultBuilder.build());
                    responseObserver.onCompleted();
                  } catch (Exception e) {
                    handleError(e, responseObserver);
                  }
                }
              });
    } catch (Exception e) {
      handleError(e, responseObserver);
    }
  }

  private void executeBatch(
      Connection connection,
      io.stargate.db.Batch preparedBatch,
      BatchParameters parameters,
      StreamObserver<Result> responseObserver) {
    try {
      long queryStartNanoTime = System.nanoTime();

      connection
          .batch(preparedBatch, makeParameters(parameters), queryStartNanoTime)
          .whenComplete(
              (result, t) -> {
                if (t != null) {
                  handleError(t, responseObserver);
                } else {
                  try {
                    Result.Builder resultBuilder = makeResultBuilder(result);
                    if (result.kind != Kind.Void) {
                      throw Status.INTERNAL.withDescription("Unhandled result kind").asException();
                    }
                    responseObserver.onNext(resultBuilder.build());
                    responseObserver.onCompleted();
                  } catch (Exception e) {
                    handleError(e, responseObserver);
                  }
                }
              });
    } catch (Exception e) {
      handleError(e, responseObserver);
    }
  }

  private BoundStatement bindValues(PayloadHandler handler, Prepared prepared, Payload payload)
      throws Exception {
    if (!payload.hasValue()) {
      return new BoundStatement(prepared.statementId, Collections.emptyList(), null);
    }
    return handler.bindValues(prepared, payload, unsetValue);
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

  private Parameters makeParameters(BatchParameters parameters) {
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

    return builder.tracingRequested(parameters.getTracing()).build();
  }

  private Connection newConnection(AuthenticatedUser user) {
    Connection connection;
    if (!user.isFromExternalAuth()) {
      SocketAddress remoteAddress = REMOTE_ADDRESS_KEY.get();
      InetSocketAddress inetSocketAddress = DUMMY_ADDRESS;
      if (remoteAddress instanceof InetSocketAddress) {
        inetSocketAddress = (InetSocketAddress) remoteAddress;
      }
      connection = persistence.newConnection(new ClientInfo(inetSocketAddress, null));
    } else {
      connection = persistence.newConnection();
    }
    connection.login(user);
    return connection;
  }

  private Result.Builder makeResultBuilder(io.stargate.db.Result result) {
    Result.Builder resultBuilder = Result.newBuilder();
    List<String> warnings = result.getWarnings();
    if (warnings != null) {
      resultBuilder.addAllWarnings(warnings);
    }
    return resultBuilder;
  }

  /**
   * Concurrently prepares queries in a batch. It'll prepare up to {@link
   * Service#MAX_CONCURRENT_PREPARES_FOR_BATCH} queries simultaneously.
   */
  private class BatchPreparer {

    private final AtomicInteger queryIndex = new AtomicInteger();
    private final Connection connection;
    private final Batch batch;
    private final List<Statement> statements;
    private final CompletableFuture<io.stargate.db.Batch> future;

    public BatchPreparer(Connection connection, Batch batch) {
      this.connection = connection;
      this.batch = batch;
      statements = Collections.synchronizedList(new ArrayList<>(batch.getQueriesCount()));
      future = new CompletableFuture<>();
    }

    /**
     * Initiates the initial prepares. When these prepares finish they'll pull the next available
     * query in the batch and prepare it.
     *
     * @return An future which completes with an internal batch statement with all queries prepared.
     */
    public CompletableFuture<io.stargate.db.Batch> prepare() {
      int numToPrepare = Math.min(batch.getQueriesCount(), MAX_CONCURRENT_PREPARES_FOR_BATCH);
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
      if (index >= batch.getQueriesCount()) {
        future.complete(
            new io.stargate.db.Batch(BatchType.fromId(batch.getTypeValue()), statements));
        return;
      }

      BatchQuery query = batch.getQueries(index);
      BatchParameters batchParameters = batch.getParameters();

      prepareQuery(
              connection,
              query.getCql(),
              batchParameters.hasKeyspace() ? batchParameters.getKeyspace().getValue() : null,
              batchParameters.getTracing())
          .whenComplete(
              (prepared, t) -> {
                if (t != null) {
                  future.completeExceptionally(t);
                } else {
                  try {
                    PayloadHandler handler = PayloadHandlers.get(query.getPayload().getType());
                    statements.add(bindValues(handler, prepared, query.getPayload()));
                    next(); // Prepare the next query in the batch
                  } catch (Exception e) {
                    future.completeExceptionally(e);
                  }
                }
              });
    }
  }
}
