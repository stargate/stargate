package io.stargate.bridge.service;

import io.grpc.stub.StreamObserver;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.db.Persistence;
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

  @Override
  protected void setSuccess(QueryOuterClass.Response response) {
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }
}
