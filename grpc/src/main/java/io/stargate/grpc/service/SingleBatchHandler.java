package io.stargate.grpc.service;

import io.grpc.stub.StreamObserver;
import io.stargate.db.Persistence;
import io.stargate.proto.QueryOuterClass;

public class SingleBatchHandler extends BatchHandler {
  private final StreamObserver<QueryOuterClass.Response> responseObserver;

  SingleBatchHandler(
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
