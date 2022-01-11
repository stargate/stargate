package io.stargate.grpc.service.streaming;

import io.stargate.db.Persistence;
import io.stargate.grpc.service.ExceptionHandler;
import io.stargate.grpc.service.MessageHandler;
import io.stargate.grpc.service.StreamingSuccessHandler;
import io.stargate.proto.QueryOuterClass;
import java.util.concurrent.ScheduledExecutorService;

/** Constructs the {@link StreamingQueryHandler}. */
public class StreamingQueryHandlerFactory
    implements StreamingHandlerFactory<QueryOuterClass.Query> {

  private final Persistence.Connection connection;
  private final Persistence persistence;
  private final ScheduledExecutorService executor;
  private final int schemaAgreementRetries;

  public StreamingQueryHandlerFactory(
      Persistence.Connection connection,
      Persistence persistence,
      ScheduledExecutorService executor,
      int schemaAgreementRetries) {
    this.connection = connection;
    this.persistence = persistence;
    this.executor = executor;
    this.schemaAgreementRetries = schemaAgreementRetries;
  }

  @Override
  public MessageHandler<QueryOuterClass.Query, ?> create(
      QueryOuterClass.Query query,
      StreamingSuccessHandler streamingSuccessHandler,
      ExceptionHandler exceptionHandler) {
    return new StreamingQueryHandler(
        query,
        connection,
        persistence,
        executor,
        schemaAgreementRetries,
        streamingSuccessHandler,
        exceptionHandler);
  }
}
