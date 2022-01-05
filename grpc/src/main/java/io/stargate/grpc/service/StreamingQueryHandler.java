package io.stargate.grpc.service;

import io.grpc.stub.StreamObserver;
import io.stargate.db.Persistence;
import io.stargate.proto.QueryOuterClass;
import java.util.concurrent.ScheduledExecutorService;

public class StreamingQueryHandler extends QueryHandler {
  private final SuccessHandler successHandler;

  StreamingQueryHandler(
      QueryOuterClass.Query query,
      Persistence.Connection connection,
      Persistence persistence,
      ScheduledExecutorService executor,
      int schemaAgreementRetries,
      StreamObserver<QueryOuterClass.Response> responseObserver,
      SuccessHandler successHandler,
      ExceptionHandler exceptionHandler) {
    super(
        query,
        connection,
        persistence,
        executor,
        schemaAgreementRetries,
        responseObserver,
        exceptionHandler);
    this.successHandler = successHandler;
  }

  @Override
  protected void setSuccess(QueryOuterClass.Response response) {
    successHandler.handleResponse(response);
  }
}
