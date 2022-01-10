package io.stargate.grpc.service.streaming;

import io.stargate.db.Persistence;
import io.stargate.grpc.service.ExceptionHandler;
import io.stargate.grpc.service.MessageHandler;
import io.stargate.grpc.service.StreamingSuccessHandler;
import io.stargate.proto.QueryOuterClass;

/** Constructs the {@link StreamingBatchHandler}. */
public class StreamingBatchHandlerFactory
    implements StreamingHandlerFactory<QueryOuterClass.Batch> {

  private final Persistence.Connection connection;
  private final Persistence persistence;

  public StreamingBatchHandlerFactory(Persistence.Connection connection, Persistence persistence) {
    this.connection = connection;
    this.persistence = persistence;
  }

  @Override
  public MessageHandler<QueryOuterClass.Batch, ?> create(
      QueryOuterClass.Batch batch,
      StreamingSuccessHandler streamingSuccessHandler,
      ExceptionHandler exceptionHandler) {
    return new StreamingBatchHandler(
        batch, connection, persistence, streamingSuccessHandler, exceptionHandler);
  }
}
