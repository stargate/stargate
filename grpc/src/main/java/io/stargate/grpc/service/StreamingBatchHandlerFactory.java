package io.stargate.grpc.service;

import io.stargate.db.Persistence;
import io.stargate.proto.QueryOuterClass;

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
      SuccessHandler successHandler,
      ExceptionHandler exceptionHandler) {
    return new StreamingBatchHandler(
        batch, connection, persistence, successHandler, exceptionHandler);
  }
}
