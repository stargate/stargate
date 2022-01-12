package io.stargate.grpc.service.streaming;

import io.stargate.db.Persistence;
import io.stargate.grpc.service.BatchHandler;
import io.stargate.grpc.service.ExceptionHandler;
import io.stargate.grpc.service.StreamingSuccessHandler;
import io.stargate.proto.QueryOuterClass;

/**
 * Handles the response for Batch queries, constructs the {@link
 * io.stargate.proto.QueryOuterClass.StreamingResponse} for the {@link
 * io.stargate.proto.QueryOuterClass.Response}. Finally, invokes the {@link StreamingSuccessHandler}
 * for it.
 */
public class StreamingBatchHandler extends BatchHandler {
  private final StreamingSuccessHandler streamingSuccessHandler;

  StreamingBatchHandler(
      QueryOuterClass.Batch batch,
      Persistence.Connection connection,
      Persistence persistence,
      StreamingSuccessHandler streamingSuccessHandler,
      ExceptionHandler exceptionHandler) {
    super(batch, connection, persistence, exceptionHandler);
    this.streamingSuccessHandler = streamingSuccessHandler;
  }

  @Override
  protected void setSuccess(QueryOuterClass.Response response) {
    streamingSuccessHandler.handleResponse(
        QueryOuterClass.StreamingResponse.newBuilder().setResponse(response).build());
  }
}
