package io.stargate.grpc.service.streaming;

import io.stargate.db.Persistence;
import io.stargate.grpc.service.BatchHandler;
import io.stargate.grpc.service.ExceptionHandler;
import io.stargate.grpc.service.SuccessHandler;
import io.stargate.proto.QueryOuterClass;

public class StreamingBatchHandler extends BatchHandler {
  private final SuccessHandler successHandler;

  StreamingBatchHandler(
      QueryOuterClass.Batch batch,
      Persistence.Connection connection,
      Persistence persistence,
      SuccessHandler successHandler,
      ExceptionHandler exceptionHandler) {
    super(batch, connection, persistence, exceptionHandler);
    this.successHandler = successHandler;
  }

  @Override
  protected void setSuccess(QueryOuterClass.Response response) {
    successHandler.handleResponse(
        QueryOuterClass.StreamingResponse.newBuilder().setResponse(response).build());
  }
}
