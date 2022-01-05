package io.stargate.grpc.service;

import io.grpc.stub.StreamObserver;
import io.stargate.db.Persistence;
import io.stargate.proto.QueryOuterClass;

public class StreamingBatchHandler extends BatchHandler {
  private final SuccessHandler successHandler;

  StreamingBatchHandler(
      QueryOuterClass.Batch batch,
      Persistence.Connection connection,
      Persistence persistence,
      StreamObserver<QueryOuterClass.Response> responseObserver,
      SuccessHandler successHandler,
      ExceptionHandler exceptionHandler) {
    super(batch, connection, persistence, responseObserver, exceptionHandler);
    this.successHandler = successHandler;
  }

  @Override
  protected void setSuccess(QueryOuterClass.Response response) {
    successHandler.handleResponse(response);
  }
}
