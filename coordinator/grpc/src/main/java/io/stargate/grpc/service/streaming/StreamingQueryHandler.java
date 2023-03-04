package io.stargate.grpc.service.streaming;

import io.stargate.db.Persistence;
import io.stargate.grpc.service.ExceptionHandler;
import io.stargate.grpc.service.QueryHandler;
import io.stargate.grpc.service.StreamingSuccessHandler;
import io.stargate.proto.QueryOuterClass;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Handles the response for normal queries, constructs the {@link
 * io.stargate.proto.QueryOuterClass.StreamingResponse} for the {@link
 * io.stargate.proto.QueryOuterClass.Response}. Finally, invokes the {@link StreamingSuccessHandler}
 * for it.
 */
public class StreamingQueryHandler extends QueryHandler {
  private final StreamingSuccessHandler streamingSuccessHandler;

  StreamingQueryHandler(
      QueryOuterClass.Query query,
      Persistence.Connection connection,
      Persistence persistence,
      ScheduledExecutorService executor,
      int schemaAgreementRetries,
      StreamingSuccessHandler streamingSuccessHandler,
      ExceptionHandler exceptionHandler) {
    super(query, connection, persistence, executor, schemaAgreementRetries, exceptionHandler);
    this.streamingSuccessHandler = streamingSuccessHandler;
  }

  @Override
  protected void setSuccess(QueryOuterClass.Response response) {
    streamingSuccessHandler.handleResponse(
        QueryOuterClass.StreamingResponse.newBuilder().setResponse(response).build());
  }
}
