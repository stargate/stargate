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
import io.stargate.db.ClientInfo;
import io.stargate.db.ImmutableParameters;
import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.Persistence.Connection;
import io.stargate.db.Result.Prepared;
import io.stargate.db.Result.Rows;
import io.stargate.grpc.payload.PayloadHandler;
import io.stargate.grpc.payload.PayloadHandlers;
import io.stargate.proto.QueryOuterClass.Payload;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.QueryParameters;
import io.stargate.proto.QueryOuterClass.Result;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.function.BiConsumer;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

public class Service extends io.stargate.proto.StargateGrpc.StargateImplBase {
  public static final Context.Key<AuthenticationSubject> AUTHENTICATION_KEY =
      Context.key("authentication");
  public static final Context.Key<SocketAddress> REMOTE_ADDRESS_KEY = Context.key("remoteAddress");

  private static final InetSocketAddress DUMMY_ADDRESS = new InetSocketAddress(9042);

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

      prepare(
          connection,
          query,
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

  private void handleError(Throwable throwable, StreamObserver<Result> responseObserver) {
    if (throwable instanceof StatusException || throwable instanceof StatusRuntimeException) {
      responseObserver.onError(throwable);
    } else {
      responseObserver.onError(Status.UNKNOWN.withCause(throwable).asRuntimeException());
    }
  }

  private void prepare(
      Connection connection, Query query, BiConsumer<Prepared, ? super Throwable> afterPrepare) {
    final StringBuilder keyBuilder = new StringBuilder(query.getCql());
    connection.loggedUser().ifPresent(user -> keyBuilder.append(user.name()));
    QueryParameters queryParameters = query.getParameters();
    if (queryParameters.getKeyspace().isInitialized()) {
      keyBuilder.append(queryParameters.getKeyspace().getValue());
    }
    final String key = keyBuilder.toString();
    // Caching here to avoid round trip to the persistence backend thread.
    Prepared prepared = preparedCache.getIfPresent(key);
    if (prepared != null) {
      afterPrepare.accept(prepared, null);
    } else {
      connection
          .prepare(
              query.getCql(),
              ImmutableParameters.builder()
                  .tracingRequested(query.getParameters().getTracing())
                  .build())
          .whenComplete(
              (p, t) -> {
                if (t == null) {
                  preparedCache.put(key, p);
                }
                afterPrepare.accept(p, t);
              });
    }
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
      if (!payload.hasValue()) {
        responseObserver.onError(
            Status.INVALID_ARGUMENT.withDescription("No payload provided").asException());
        return;
      }

      PayloadHandler handler = PayloadHandlers.HANDLERS.get(payload.getType());
      if (handler == null) {
        responseObserver.onError(
            Status.UNIMPLEMENTED.withDescription("Unsupported payload type").asException());
        return;
      }

      connection
          .execute(
              handler.bindValues(prepared, payload, unsetValue),
              makeParameters(parameters),
              queryStartNanoTime)
          .whenComplete(
              (result, t) -> {
                if (t != null) {
                  handleError(t, responseObserver);
                } else {
                  try {
                    Result.Builder resultBuilder = Result.newBuilder();
                    switch (result.kind) {
                      case Void:
                        break;
                      case Rows:
                        resultBuilder.setPayload(handler.processResult((Rows) result, parameters));
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
}
