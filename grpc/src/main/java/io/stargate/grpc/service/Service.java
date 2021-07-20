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
import com.google.protobuf.Any;
import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.ProtoUtils;
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
import io.stargate.db.Result;
import io.stargate.db.Result.Kind;
import io.stargate.db.Result.Prepared;
import io.stargate.db.Result.Rows;
import io.stargate.db.Statement;
import io.stargate.db.tracing.QueryTracingFetcher;
import io.stargate.grpc.payload.PayloadHandler;
import io.stargate.grpc.payload.PayloadHandlers;
import io.stargate.grpc.tracing.TraceEventsMapper;
import io.stargate.proto.QueryOuterClass.AlreadyExists;
import io.stargate.proto.QueryOuterClass.Batch;
import io.stargate.proto.QueryOuterClass.BatchParameters;
import io.stargate.proto.QueryOuterClass.BatchQuery;
import io.stargate.proto.QueryOuterClass.CasWriteUnknown;
import io.stargate.proto.QueryOuterClass.FunctionFailure;
import io.stargate.proto.QueryOuterClass.Payload;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.QueryParameters;
import io.stargate.proto.QueryOuterClass.ReadFailure;
import io.stargate.proto.QueryOuterClass.ReadTimeout;
import io.stargate.proto.QueryOuterClass.Response;
import io.stargate.proto.QueryOuterClass.Unavailable;
import io.stargate.proto.QueryOuterClass.WriteFailure;
import io.stargate.proto.QueryOuterClass.WriteTimeout;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import javax.annotation.Nullable;
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
import org.immutables.value.Value;
import org.jetbrains.annotations.NotNull;

public class Service extends io.stargate.proto.StargateGrpc.StargateImplBase {

  public static final Context.Key<AuthenticationSubject> AUTHENTICATION_KEY =
      Context.key("authentication");
  public static final Context.Key<SocketAddress> REMOTE_ADDRESS_KEY = Context.key("remoteAddress");
  public static final ConsistencyLevel DEFAULT_TRACING_CONSISTENCY = ConsistencyLevel.ONE;

  public static Key<Unavailable> UNAVAILABLE_KEY =
      ProtoUtils.keyForProto(Unavailable.getDefaultInstance());
  public static Key<WriteTimeout> WRITE_TIMEOUT_KEY =
      ProtoUtils.keyForProto(WriteTimeout.getDefaultInstance());
  public static Key<ReadTimeout> READ_TIMEOUT_KEY =
      ProtoUtils.keyForProto(ReadTimeout.getDefaultInstance());
  public static Key<ReadFailure> READ_FAILURE_KEY =
      ProtoUtils.keyForProto(ReadFailure.getDefaultInstance());
  public static Key<FunctionFailure> FUNCTION_FAILURE_KEY =
      ProtoUtils.keyForProto(FunctionFailure.getDefaultInstance());
  public static Key<WriteFailure> WRITE_FAILURE_KEY =
      ProtoUtils.keyForProto(WriteFailure.getDefaultInstance());
  public static Key<AlreadyExists> ALREADY_EXISTS_KEY =
      ProtoUtils.keyForProto(AlreadyExists.getDefaultInstance());
  public static Key<CasWriteUnknown> CAS_WRITE_UNKNOWN_KEY =
      ProtoUtils.keyForProto(CasWriteUnknown.getDefaultInstance());

  private static final InetSocketAddress DUMMY_ADDRESS = new InetSocketAddress(9042);

  /** The maximum number of batch queries to prepare simultaneously. */
  private static final int MAX_CONCURRENT_PREPARES_FOR_BATCH =
      Math.max(Integer.getInteger("stargate.grpc.max_concurrent_prepares_for_batch", 1), 1);

  // TODO: Add a maximum size and add tuning options
  private final Cache<PrepareInfo, Prepared> preparedCache = Caffeine.newBuilder().build();

  private final Persistence persistence;
  private final ByteBuffer unsetValue;

  @SuppressWarnings("unused")
  private final Metrics metrics;

  /** Used as key for the the local prepare cache. */
  @Value.Immutable
  interface PrepareInfo {

    @Nullable
    String keyspace();

    @Nullable
    String user();

    String cql();
  }

  public Service(Persistence persistence, Metrics metrics) {
    this.persistence = persistence;
    this.metrics = metrics;
    assert this.metrics != null;
    unsetValue = persistence.unsetValue();
  }

  @Override
  public void executeQuery(Query query, StreamObserver<Response> responseObserver) {
    try {
      AuthenticationSubject authenticationSubject = AUTHENTICATION_KEY.get();
      Connection connection = newConnection(authenticationSubject.asUser());
      QueryParameters queryParameters = query.getParameters();

      PrepareInfo prepareInfo =
          ImmutablePrepareInfo.builder()
              .keyspace(
                  queryParameters.hasKeyspace() ? queryParameters.getKeyspace().getValue() : null)
              .user(connection.loggedUser().map(AuthenticatedUser::name).orElse(null))
              .cql(query.getCql())
              .build();

      prepareQuery(connection, prepareInfo, queryParameters.getTracing())
          .whenComplete(
              (prepared, t) -> {
                if (t != null) {
                  handleException(t, responseObserver);
                } else {
                  executePrepared(connection, prepared, query, responseObserver, prepareInfo);
                }
              });
    } catch (Throwable t) {
      handleException(t, responseObserver);
    }
  }

  @Override
  public void executeBatch(Batch batch, StreamObserver<Response> responseObserver) {
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
                  handleException(t, responseObserver);
                } else {
                  executeBatch(connection, preparedBatch, batch, responseObserver);
                }
              });

    } catch (Throwable t) {
      handleException(t, responseObserver);
    }
  }

  private void handleException(Throwable throwable, StreamObserver<?> responseObserver) {
    if (throwable instanceof StatusException || throwable instanceof StatusRuntimeException) {
      responseObserver.onError(throwable);
    } else if (throwable instanceof PersistenceException) {
      handlePersistenceException((PersistenceException) throwable, responseObserver);
    } else {
      responseObserver.onError(
          Status.UNKNOWN
              .withDescription(throwable.getMessage())
              .withCause(throwable)
              .asRuntimeException());
    }
  }

  private void handlePersistenceException(
      PersistenceException pe, StreamObserver<?> responseObserver) {
    switch (pe.code()) {
      case SERVER_ERROR:
      case PROTOCOL_ERROR: // Fallthrough
      case UNPREPARED: // Fallthrough
        onError(responseObserver, Status.INTERNAL, pe);
        break;
      case INVALID:
      case SYNTAX_ERROR: // Fallthrough
        onError(responseObserver, Status.INVALID_ARGUMENT, pe);
        break;
      case TRUNCATE_ERROR:
      case CDC_WRITE_FAILURE: // Fallthrough
        onError(responseObserver, Status.ABORTED, pe);
        break;
      case BAD_CREDENTIALS:
        onError(responseObserver, Status.UNAUTHENTICATED, pe);
        break;
      case UNAVAILABLE:
        UnavailableException ue = (UnavailableException) pe;
        onError(
            responseObserver,
            Status.UNAVAILABLE,
            ue,
            makeTrailer(
                UNAVAILABLE_KEY,
                Unavailable.newBuilder()
                    .setConsistencyValue(ue.consistency.code)
                    .setAlive(ue.alive)
                    .setRequired(ue.required)
                    .build()));
        break;
      case OVERLOADED:
        onError(responseObserver, Status.RESOURCE_EXHAUSTED, pe);
        break;
      case IS_BOOTSTRAPPING:
        onError(responseObserver, Status.UNAVAILABLE, pe);
        break;
      case WRITE_TIMEOUT:
        WriteTimeoutException wte = (WriteTimeoutException) pe;
        onError(
            responseObserver,
            Status.DEADLINE_EXCEEDED,
            pe,
            makeTrailer(
                WRITE_TIMEOUT_KEY,
                WriteTimeout.newBuilder()
                    .setConsistencyValue(wte.consistency.code)
                    .setBlockFor(wte.blockFor)
                    .setReceived(wte.received)
                    .setWriteType(wte.writeType.name())
                    .build()));
        break;
      case READ_TIMEOUT:
        ReadTimeoutException rte = (ReadTimeoutException) pe;
        onError(
            responseObserver,
            Status.DEADLINE_EXCEEDED,
            pe,
            makeTrailer(
                READ_TIMEOUT_KEY,
                ReadTimeout.newBuilder()
                    .setConsistencyValue(rte.consistency.code)
                    .setBlockFor(rte.blockFor)
                    .setReceived(rte.received)
                    .setDataPresent(rte.dataPresent)
                    .build()));
        break;
      case READ_FAILURE:
        ReadFailureException rfe = (ReadFailureException) pe;
        onError(
            responseObserver,
            Status.ABORTED,
            pe,
            makeTrailer(
                READ_FAILURE_KEY,
                ReadFailure.newBuilder()
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
            responseObserver,
            Status.FAILED_PRECONDITION,
            pe,
            makeTrailer(
                FUNCTION_FAILURE_KEY,
                FunctionFailure.newBuilder()
                    .setKeyspace(fee.functionName.keyspace)
                    .setFunction(fee.functionName.name)
                    .addAllArgTypes(fee.argTypes)
                    .build()));
        break;
      case WRITE_FAILURE:
        WriteFailureException wfe = (WriteFailureException) pe;
        onError(
            responseObserver,
            Status.ABORTED,
            pe,
            makeTrailer(
                WRITE_FAILURE_KEY,
                WriteFailure.newBuilder()
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
            responseObserver,
            Status.ABORTED,
            pe,
            makeTrailer(
                CAS_WRITE_UNKNOWN_KEY,
                CasWriteUnknown.newBuilder()
                    .setConsistencyValue(cwe.consistency.code)
                    .setBlockFor(cwe.blockFor)
                    .setReceived(cwe.received)
                    .build()));
        break;
      case UNAUTHORIZED:
        onError(responseObserver, Status.PERMISSION_DENIED, pe);
        break;
      case CONFIG_ERROR:
        onError(responseObserver, Status.FAILED_PRECONDITION, pe);
        break;
      case ALREADY_EXISTS:
        AlreadyExistsException aee = (AlreadyExistsException) pe;
        onError(
            responseObserver,
            Status.ALREADY_EXISTS,
            pe,
            makeTrailer(
                ALREADY_EXISTS_KEY,
                AlreadyExists.newBuilder().setKeyspace(aee.ksName).setTable(aee.cfName).build()));
        break;
      default:
        onError(responseObserver, Status.UNKNOWN, pe);
        break;
    }
  }

  private void onError(
      StreamObserver<?> responseObserver, Status status, Throwable throwable, Metadata trailer) {
    status = status.withDescription(throwable.getMessage()).withCause(throwable);
    responseObserver.onError(
        trailer != null ? status.asRuntimeException(trailer) : status.asRuntimeException());
  }

  public void onError(StreamObserver<?> responseObserver, Status status, Throwable throwable) {
    onError(responseObserver, status, throwable, null);
  }

  private <T> Metadata makeTrailer(Key<T> key, T value) {
    Metadata trailer = new Metadata();
    trailer.put(key, value);
    return trailer;
  }

  private CompletableFuture<Prepared> prepareQuery(
      Connection connection, PrepareInfo prepareInfo, boolean tracing) {
    return prepareQuery(connection, prepareInfo, tracing, false);
  }

  private CompletableFuture<Prepared> prepareQuery(
      Connection connection, PrepareInfo prepareInfo, boolean tracing, boolean shouldInvalidate) {
    CompletableFuture<Prepared> future = new CompletableFuture<>();
    Prepared prepared = null;

    // In the event a query is being retried due to a PreparedQueryNotFoundException invalidate the
    // local cache to refresh with the remote cache
    if (shouldInvalidate) {
      preparedCache.invalidate(prepareInfo);
    } else {
      // Caching here to avoid round trip to the persistence backend thread.
      prepared = preparedCache.getIfPresent(prepareInfo);
      if (prepared != null) {
        future.complete(prepared);
      }
    }

    if (prepared == null) {
      ImmutableParameters.Builder parameterBuilder =
          ImmutableParameters.builder().tracingRequested(tracing);
      String keyspace = prepareInfo.keyspace();
      if (keyspace != null) {
        parameterBuilder.defaultKeyspace(keyspace);
      }
      connection
          .prepare(prepareInfo.cql(), parameterBuilder.build())
          .whenComplete(
              (p, t) -> {
                if (t != null) {
                  future.completeExceptionally(t);
                } else {
                  preparedCache.put(prepareInfo, p);
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
      StreamObserver<Response> responseObserver,
      PrepareInfo prepareInfo) {
    try {
      long queryStartNanoTime = System.nanoTime();

      Payload values = query.getValues();
      PayloadHandler handler = PayloadHandlers.get(values.getType());

      QueryParameters parameters = query.getParameters();

      connection
          .execute(
              bindValues(handler, prepared, values), makeParameters(parameters), queryStartNanoTime)
          .handle(
              handleQuery(
                  query,
                  responseObserver,
                  handler,
                  connection,
                  prepareInfo,
                  parameters.getTracing()))
          .whenComplete(
              executeTracingQueryIfNeeded(
                  connection,
                  responseObserver,
                  parameters.getTracing(),
                  getTracingConsistency(parameters)));
    } catch (Throwable t) {
      handleException(t, responseObserver);
    }
  }

  @NotNull
  private BiConsumer<ResponseAndTraceId, Throwable> executeTracingQueryIfNeeded(
      Connection connection,
      StreamObserver<Response> responseObserver,
      boolean tracingEnabled,
      ConsistencyLevel consistencyLevel) {
    return (responseAndTraceId, t) -> {
      if (t != null) {
        handleException(t, responseObserver);
      } else if (!tracingEnabled || responseAndTraceId.tracingIdIsEmpty()) {
        // tracing is not enabled or not present, fill the response observer immediately
        Response response = responseAndTraceId.responseBuilder.build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
      } else {
        try {
          new QueryTracingFetcher(responseAndTraceId.tracingId, connection, consistencyLevel)
              .fetch()
              .whenComplete(
                  (traces, throwable) -> {
                    if (throwable != null) {
                      handleException(throwable, responseObserver);
                    } else {
                      responseAndTraceId.responseBuilder.setTraces(
                          TraceEventsMapper.toTraceEvents(
                              traces, responseAndTraceId.responseBuilder.getTraces().getId()));
                      responseObserver.onNext(responseAndTraceId.responseBuilder.build());
                      responseObserver.onCompleted();
                    }
                  });
        } catch (Throwable throwable) {
          handleException(throwable, responseObserver);
        }
      }
    };
  }

  @NotNull
  private BiFunction<Result, Throwable, ResponseAndTraceId> handleQuery(
      Query query,
      StreamObserver<Response> responseObserver,
      PayloadHandler handler,
      Connection connection,
      PrepareInfo prepareInfo,
      boolean tracingEnabled) {
    return (result, t) -> {
      if (t != null) {
        if (t instanceof PreparedQueryNotFoundException) {
          prepareQuery(connection, prepareInfo, tracingEnabled, true)
              .whenComplete(
                  (p, t1) -> {
                    if (t1 != null) {
                      handleException(t1, responseObserver);
                    } else {
                      executePrepared(connection, p, query, responseObserver, prepareInfo);
                    }
                  });
        } else {
          handleException(t, responseObserver);
        }
      } else {
        try {
          ResponseAndTraceId responseAndTraceId = new ResponseAndTraceId();
          Response.Builder responseBuilder = makeResponseBuilder(result);
          switch (result.kind) {
            case Void:
              // fill tracing id for queries that doesn't return any data (i.e. INSERT)
              handleTraceId(result.getTracingId(), query.getParameters(), responseAndTraceId);
              break;
            case SchemaChange:
              break;
            case Rows:
              responseBuilder.setResultSet(
                  Payload.newBuilder()
                      .setType(query.getValues().getType())
                      .setData(handler.processResult((Rows) result, query.getParameters())));
              handleTraceId(result.getTracingId(), query.getParameters(), responseAndTraceId);
              break;
            case SetKeyspace:
              throw Status.INVALID_ARGUMENT
                  .withDescription("USE <keyspace> not supported")
                  .asException();
            default:
              throw Status.INTERNAL.withDescription("Unhandled result kind").asException();
          }
          responseAndTraceId.setResponseBuilder(responseBuilder);
          return responseAndTraceId;
        } catch (Throwable th) {
          handleException(th, responseObserver);
        }
      }
      return new ResponseAndTraceId(makeResponseBuilder(result));
    };
  }

  private void handleTraceId(
      UUID tracingId, QueryParameters parameters, ResponseAndTraceId responseAndTraceId) {
    handleTraceId(tracingId, parameters.getTracing(), responseAndTraceId);
  }

  private void handleTraceId(
      UUID tracingId, BatchParameters parameters, ResponseAndTraceId responseAndTraceId) {
    handleTraceId(tracingId, parameters.getTracing(), responseAndTraceId);
  }

  private void handleTraceId(
      UUID tracingId, boolean tracingEnabled, ResponseAndTraceId responseAndTraceId) {
    if (tracingEnabled && tracingId != null) {
      responseAndTraceId.setTracingId(tracingId);
    }
  }

  private void executeBatch(
      Connection connection,
      io.stargate.db.Batch preparedBatch,
      Batch batch,
      StreamObserver<Response> responseObserver) {
    try {
      long queryStartNanoTime = System.nanoTime();
      BatchParameters parameters = batch.getParameters();

      connection
          .batch(preparedBatch, makeParameters(parameters), queryStartNanoTime)
          .handle(handleBatchQuery(parameters, responseObserver, connection, batch))
          .whenComplete(
              executeTracingQueryIfNeeded(
                  connection,
                  responseObserver,
                  parameters.getTracing(),
                  getTracingConsistency(parameters)));
    } catch (Throwable t) {
      handleException(t, responseObserver);
    }
  }

  private ConsistencyLevel getTracingConsistency(QueryParameters parameters) {
    if (parameters.hasTracingConsistency()) {
      return ConsistencyLevel.fromCode(parameters.getTracingConsistency().getValue().getNumber());
    } else {
      return DEFAULT_TRACING_CONSISTENCY;
    }
  }

  private ConsistencyLevel getTracingConsistency(BatchParameters parameters) {
    if (parameters.hasTracingConsistency()) {
      return ConsistencyLevel.fromCode(parameters.getTracingConsistency().getValue().getNumber());
    } else {
      return DEFAULT_TRACING_CONSISTENCY;
    }
  }

  @NotNull
  private BiFunction<Result, Throwable, ResponseAndTraceId> handleBatchQuery(
      BatchParameters parameters,
      StreamObserver<Response> responseObserver,
      Connection connection,
      Batch batch) {
    return (result, t) -> {
      if (t != null) {
        if (t instanceof PreparedQueryNotFoundException) {
          new BatchPreparer(connection, batch)
              .prepareForRetry()
              .whenComplete(
                  (p, t1) -> {
                    if (t1 != null) {
                      handleException(t1, responseObserver);
                    } else {
                      executeBatch(connection, p, batch, responseObserver);
                    }
                  });
        } else {
          handleException(t, responseObserver);
        }
      } else {
        try {
          ResponseAndTraceId responseAndTraceId = new ResponseAndTraceId();
          Response.Builder responseBuilder = makeResponseBuilder(result);
          handleTraceId(result.getTracingId(), parameters, responseAndTraceId);

          if (result.kind != Kind.Void && result.kind != Kind.Rows) {
            throw Status.INTERNAL.withDescription("Unhandled result kind").asException();
          }

          if (result.kind == Kind.Rows) {
            Payload.Type type = validateAndGetTypeForBatchQueries(batch);
            PayloadHandler handler = PayloadHandlers.get(type);
            Any data = handler.processResult((Rows) result, false);
            responseBuilder.setResultSet(Payload.newBuilder().setType(type).setData(data));
          }

          responseAndTraceId.setResponseBuilder(responseBuilder);
          return responseAndTraceId;
        } catch (Throwable th) {
          handleException(th, responseObserver);
        }
      }
      return new ResponseAndTraceId(makeResponseBuilder(result));
    };
  }

  private Payload.Type validateAndGetTypeForBatchQueries(Batch batch) {
    // we assume that all queries within a batch have the same type
    Payload.Type type = batch.getQueries(0).getValues().getType();
    boolean allTypesMatch =
        batch.getQueriesList().stream().allMatch(v -> v.getValues().getType().equals(type));
    if (!allTypesMatch) {
      throw new IllegalStateException(
          "Types for all queries within batch must be the same, and equal to: " + type);
    }

    return type;
  }

  private BoundStatement bindValues(PayloadHandler handler, Prepared prepared, Payload values)
      throws Exception {
    if (!values.hasData()) {
      return new BoundStatement(prepared.statementId, Collections.emptyList(), null);
    }
    return handler.bindValues(prepared, values.getData(), unsetValue);
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

  private Response.Builder makeResponseBuilder(io.stargate.db.Result result) {
    Response.Builder resultBuilder = Response.newBuilder();
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
        next(false);
      }
      return future;
    }

    public CompletableFuture<io.stargate.db.Batch> prepareForRetry() {
      int numToPrepare = Math.min(batch.getQueriesCount(), MAX_CONCURRENT_PREPARES_FOR_BATCH);
      assert numToPrepare != 0;
      for (int i = 0; i < numToPrepare; ++i) {
        next(true);
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

      prepareQuery(connection, prepareInfo, batchParameters.getTracing(), shouldInvalidate)
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

  private static class ResponseAndTraceId {

    @Nullable private UUID tracingId;
    private Response.Builder responseBuilder;

    public ResponseAndTraceId() {}

    public ResponseAndTraceId(Response.Builder responseBuilder) {
      this.responseBuilder = responseBuilder;
    }

    public void setTracingId(UUID tracingId) {
      this.tracingId = tracingId;
    }

    public void setResponseBuilder(Response.Builder responseBuilder) {
      this.responseBuilder = responseBuilder;
    }

    public boolean tracingIdIsEmpty() {
      return tracingId == null || tracingId.toString().isEmpty();
    }
  }
}
