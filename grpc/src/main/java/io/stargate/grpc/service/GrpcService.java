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
import io.grpc.stub.StreamObserver;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.db.AuthenticatedUser;
import io.stargate.db.ClientInfo;
import io.stargate.db.Persistence;
import io.stargate.db.Persistence.Connection;
import io.stargate.db.Result;
import io.stargate.db.Result.Prepared;
import io.stargate.proto.QueryOuterClass.Batch;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.Response;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.Nullable;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.immutables.value.Value;

public class GrpcService extends io.stargate.proto.StargateGrpc.StargateImplBase {

  public static final Context.Key<AuthenticationSubject> AUTHENTICATION_KEY =
      Context.key("authentication");
  public static final Context.Key<SocketAddress> REMOTE_ADDRESS_KEY = Context.key("remoteAddress");
  public static final Context.Key<Map<String, String>> HEADERS_KEY = Context.key("headers");
  public static final int DEFAULT_PAGE_SIZE = 100;
  public static final ConsistencyLevel DEFAULT_CONSISTENCY = ConsistencyLevel.LOCAL_QUORUM;
  public static final ConsistencyLevel DEFAULT_SERIAL_CONSISTENCY = ConsistencyLevel.SERIAL;

  private static final InetSocketAddress DUMMY_ADDRESS = new InetSocketAddress(9042);

  // TODO: Add a maximum size and add tuning options
  private final Cache<PrepareInfo, CompletionStage<Prepared>> preparedCache =
      Caffeine.newBuilder().build();

  private final Persistence persistence;

  @SuppressWarnings("unused")
  private final Metrics metrics;

  private final ScheduledExecutorService executor;

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
    this.persistence = persistence;
    this.metrics = metrics;
    this.executor = executor;
    assert this.metrics != null;
  }

  @Override
  public void executeQuery(Query query, StreamObserver<Response> responseObserver) {
    newConnection(responseObserver)
        .ifPresent(
            connection ->
                new QueryHandler(
                        query, connection, preparedCache, persistence, executor, responseObserver)
                    .handle());
  }

  @Override
  public void executeBatch(Batch batch, StreamObserver<Response> responseObserver) {
    newConnection(responseObserver)
        .ifPresent(
            connection ->
                new BatchHandler(batch, connection, preparedCache, persistence, responseObserver)
                    .handle());
  }

  private Optional<Connection> newConnection(StreamObserver<Response> responseObserver) {
    try {
      AuthenticationSubject authenticationSubject = AUTHENTICATION_KEY.get();
      AuthenticatedUser user = authenticationSubject.asUser();
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
      if (user.token() != null) {
        connection.clientInfo().ifPresent(c -> c.setAuthenticatedUser(user));
      }
      Map<String, String> headers = HEADERS_KEY.get();
      connection.setCustomProperties(headers);
      return Optional.of(connection);
    } catch (Throwable throwable) {
      responseObserver.onError(
          Status.UNKNOWN
              .withDescription(throwable.getMessage())
              .withCause(throwable)
              .asRuntimeException());
      return Optional.empty();
    }
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
