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
import io.grpc.stub.StreamObserver;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.db.Persistence;
import io.stargate.db.Persistence.Connection;
import io.stargate.db.Result;
import io.stargate.db.Result.Prepared;
import io.stargate.proto.QueryOuterClass.Batch;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.Response;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.Nullable;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.immutables.value.Value;

public class GrpcService extends io.stargate.proto.StargateGrpc.StargateImplBase {

  public static final Context.Key<Connection> CONNECTION_KEY = Context.key("connection");
  public static final Context.Key<Map<String, String>> HEADERS_KEY = Context.key("headers");
  public static final int DEFAULT_PAGE_SIZE = 100;
  public static final ConsistencyLevel DEFAULT_CONSISTENCY = ConsistencyLevel.LOCAL_QUORUM;
  public static final ConsistencyLevel DEFAULT_SERIAL_CONSISTENCY = ConsistencyLevel.SERIAL;

  // TODO: Add a maximum size and add tuning options
  private final Cache<PrepareInfo, CompletionStage<Prepared>> preparedCache =
      Caffeine.newBuilder().build();

  private final Persistence persistence;

  @SuppressWarnings("unused")
  private final Metrics metrics;

  private final ScheduledExecutorService executor;
  private final int schemaAgreementRetries;

  /** Used as key for the the local prepare cache. */
  @Value.Immutable
  interface PrepareInfo {

    @Nullable
    String keyspace();

    @Nullable
    String user();

    String cql();
  }

  public GrpcService(Persistence persistence, Metrics metrics, ScheduledExecutorService executor) {
    this(persistence, metrics, executor, Persistence.SCHEMA_AGREEMENT_WAIT_RETRIES);
  }

  GrpcService(
      Persistence persistence,
      Metrics metrics,
      ScheduledExecutorService executor,
      int schemaAgreementRetries) {
    this.persistence = persistence;
    this.metrics = metrics;
    this.executor = executor;
    this.schemaAgreementRetries = schemaAgreementRetries;
    assert this.metrics != null;
  }

  @Override
  public void executeQuery(Query query, StreamObserver<Response> responseObserver) {
    new QueryHandler(
            query,
            CONNECTION_KEY.get(),
            preparedCache,
            persistence,
            executor,
            schemaAgreementRetries,
            responseObserver)
        .handle();
  }

  @Override
  public void executeBatch(Batch batch, StreamObserver<Response> responseObserver) {
    new BatchHandler(batch, CONNECTION_KEY.get(), preparedCache, persistence, responseObserver)
        .handle();
  }

  static class ResponseAndTraceId {

    final @Nullable UUID tracingId;
    final Response.Builder responseBuilder;

    static ResponseAndTraceId from(Result result, Response.Builder responseBuilder) {
      return new ResponseAndTraceId(result.getTracingId(), responseBuilder);
    }

    private ResponseAndTraceId(@Nullable UUID tracingId, Response.Builder responseBuilder) {
      this.tracingId = tracingId;
      this.responseBuilder = responseBuilder;
    }

    public boolean tracingIdIsEmpty() {
      return tracingId == null || tracingId.toString().isEmpty();
    }
  }
}
