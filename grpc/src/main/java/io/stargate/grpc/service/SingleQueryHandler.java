package io.stargate.grpc.service;

import io.grpc.stub.StreamObserver;
import io.stargate.db.Persistence;
import io.stargate.proto.QueryOuterClass;
import java.util.concurrent.ScheduledExecutorService;

public class SingleQueryHandler extends QueryHandler {
  private final StreamObserver<QueryOuterClass.Response> responseObserver;

  public SingleQueryHandler(
      QueryOuterClass.Query query,
      Persistence.Connection connection,
      Persistence persistence,
      ScheduledExecutorService executor,
      int schemaAgreementRetries,
      StreamObserver<QueryOuterClass.Response> responseObserver,
      ExceptionHandler exceptionHandler) {
    super(query, connection, persistence, executor, schemaAgreementRetries, exceptionHandler);
    this.responseObserver = responseObserver;
  }

  public SingleQueryHandler(
      QueryOuterClass.Query query,
      Persistence.Connection connection,
      Persistence persistence,
      ScheduledExecutorService executor,
      int schemaAgreementRetries,
      StreamObserver<QueryOuterClass.Response> responseObserver,
      ExceptionHandler exceptionHandler,
      boolean enrichResponse) {
    super(
        query,
        connection,
        persistence,
        executor,
        schemaAgreementRetries,
        exceptionHandler,
        enrichResponse);
    this.responseObserver = responseObserver;
  }

  @Override
  protected void setSuccess(QueryOuterClass.Response response) {
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }
}
