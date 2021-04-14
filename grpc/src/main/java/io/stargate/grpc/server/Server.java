package io.stargate.grpc.server;

import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.db.AuthenticatedUser;
import io.stargate.db.ClientInfo;
import io.stargate.db.ImmutableParameters;
import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.Persistence.Connection;
import io.stargate.db.SimpleStatement;
import io.stargate.proto.QueryOuterClass.Empty;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.Result;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class Server extends io.stargate.proto.StargateGrpc.StargateImplBase {
  public static final Context.Key<AuthenticationSubject> AUTHENTICATION_KEY =
      Context.key("authentication");
  public static final Context.Key<SocketAddress> REMOTE_ADDRESS_KEY = Context.key("remoteAddress");

  private final Persistence persistence;

  @SuppressWarnings("unused")
  private final Metrics metrics;

  public Server(Persistence persistence, Metrics metrics) {
    this.persistence = persistence;
    this.metrics = metrics;
  }

  @Override
  public void execute(Query query, StreamObserver<Result> responseObserver) {
    long queryStartNanoTime = System.nanoTime();
    Parameters parameters = ImmutableParameters.builder().build();

    try {
      AuthenticationSubject authenticationSubject = AUTHENTICATION_KEY.get();
      Connection connection = newConnection(authenticationSubject.asUser());
      connection
          .execute(new SimpleStatement(query.getCql()), parameters, queryStartNanoTime)
          .whenComplete(
              (r, t) -> {
                if (t != null) {
                  responseObserver.onError(t);
                } else {
                  responseObserver.onNext(
                      Result.newBuilder().setEmpty(Empty.newBuilder().build()).build());
                  responseObserver.onCompleted();
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
