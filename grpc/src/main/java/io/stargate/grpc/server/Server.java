package io.stargate.grpc.server;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.db.AuthenticatedUser;
import io.stargate.db.ClientInfo;
import io.stargate.db.ImmutableParameters;
import io.stargate.db.Persistence;
import io.stargate.db.Persistence.Connection;
import io.stargate.db.Result.Prepared;
import io.stargate.db.Result.Rows;
import io.stargate.grpc.payload.PayloadHandler;
import io.stargate.grpc.payload.PayloadHandlers;
import io.stargate.proto.QueryOuterClass.Empty;
import io.stargate.proto.QueryOuterClass.Payload;
import io.stargate.proto.QueryOuterClass.Payload.Type;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.QueryParameters;
import io.stargate.proto.QueryOuterClass.Result;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.function.BiConsumer;

public class Server extends io.stargate.proto.StargateGrpc.StargateImplBase {
  public static final Context.Key<AuthenticationSubject> AUTHENTICATION_KEY =
      Context.key("authentication");
  public static final Context.Key<SocketAddress> REMOTE_ADDRESS_KEY = Context.key("remoteAddress");

  // TODO: Add a maximum size and add tuning options
  private final Cache<String, Prepared> preparedCache = Caffeine.newBuilder().build();

  private final Persistence persistence;

  @SuppressWarnings("unused")
  private final Metrics metrics;

  public Server(Persistence persistence, Metrics metrics) {
    this.persistence = persistence;
    this.metrics = metrics;
  }

  @Override
  public void execute(Query query, StreamObserver<Result> responseObserver) {
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
    // TODO: Do something better here. Log and maybe return as a response type instead of using
    // `onError()`.
    responseObserver.onError(throwable);
  }

  private void prepare(
      Connection connection, Query query, BiConsumer<Prepared, ? super Throwable> afterPrepare) {
    final StringBuilder keyBuilder = new StringBuilder(query.getCql());
    connection.loggedUser().ifPresent(user -> keyBuilder.append(user.name()));
    String keyspace = query.getParameters().getKeyspace();
    if (keyspace != null) {
      keyBuilder.append(keyspace);
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

      Payload.Type payloadType = payload.getType() == null ? Type.TYPE_CQL : payload.getType();

      PayloadHandler handler = PayloadHandlers.HANDLERS.get(payloadType);
      if (handler == null) {
        handleError(new IllegalArgumentException("Unsupported payload type"), responseObserver);
        return;
      }

      connection
          .execute(
              handler.bindValues(prepared, payload),
              ImmutableParameters.builder().build(), // TODO: Build parameters
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
                        resultBuilder.setEmpty(Empty.newBuilder().build());
                        break;
                      case Rows:
                        resultBuilder.setPayload(handler.processResult((Rows) result));
                        break;
                      case SchemaChange:
                        // TODO: Wait for schema agreement, etc.
                        persistence.waitForSchemaAgreement(); // TODO: Could this be made
                        // async? This is blocking the
                        // gRPC thread.
                        resultBuilder.setEmpty(Empty.newBuilder().build());
                        break;
                      case SetKeyspace:
                        // TODO: Prevent "USE <keyspace>" from happening
                        throw new RuntimeException("USE <keyspace> not supported");
                      default:
                        throw new RuntimeException("Unhandled result kind");
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

  private Connection newConnection(AuthenticatedUser user) {
    Connection connection;
    if (!user.isFromExternalAuth()) {
      connection =
          persistence.newConnection(
              new ClientInfo((InetSocketAddress) REMOTE_ADDRESS_KEY.get(), null));
    } else {
      connection = persistence.newConnection();
    }
    connection.login(user);
    return connection;
  }
}
