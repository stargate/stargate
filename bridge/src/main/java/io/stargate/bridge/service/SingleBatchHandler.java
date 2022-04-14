package io.stargate.bridge.service;

import io.grpc.stub.StreamObserver;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.db.Persistence;

public class SingleBatchHandler extends BatchHandler {
  private final StreamObserver<QueryOuterClass.Response> responseObserver;

  public SingleBatchHandler(
      QueryOuterClass.Batch batch,
      Persistence.Connection connection,
      Persistence persistence,
      StreamObserver<QueryOuterClass.Response> responseObserver,
      ExceptionHandler exceptionHandler) {
    super(batch, connection, persistence, exceptionHandler);
    this.responseObserver = responseObserver;
  }

  @Override
  protected void setSuccess(QueryOuterClass.Response response) {
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }
}
