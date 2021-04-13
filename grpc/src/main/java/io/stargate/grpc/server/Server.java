package io.stargate.grpc.server;

import io.grpc.stub.StreamObserver;
import io.stargate.auth.AuthenticationService;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.db.ImmutableParameters;
import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.SimpleStatement;
import io.stargate.proto.QueryOuterClass.Empty;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.Result;

public class Server extends io.stargate.proto.StargateGrpc.StargateImplBase {

  private final Persistence persistence;
  private final Metrics metrics;
  private final AuthenticationService authenticationService;

  public Server(
      Persistence persistence, Metrics metrics, AuthenticationService authenticationService) {
    this.persistence = persistence;
    this.metrics = metrics;
    this.authenticationService = authenticationService;
  }

  @Override
  public void execute(Query query, StreamObserver<Result> responseObserver) {
    long queryStartNanoTime = System.nanoTime();
    Parameters parameters = ImmutableParameters.builder().build();
    persistence
        .newConnection()
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
  }
}
