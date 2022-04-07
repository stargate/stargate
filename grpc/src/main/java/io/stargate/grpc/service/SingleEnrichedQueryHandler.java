package io.stargate.grpc.service;

import io.grpc.stub.StreamObserver;
import io.stargate.db.Persistence;
import io.stargate.proto.BridgeQuery.EnrichedResponse;
import io.stargate.proto.QueryOuterClass;
import java.util.concurrent.ScheduledExecutorService;

public class SingleEnrichedQueryHandler extends EnrichedQueryHandler {
  private final StreamObserver<EnrichedResponse> responseObserver;

  public SingleEnrichedQueryHandler(
      QueryOuterClass.Query query,
      Persistence.Connection connection,
      Persistence persistence,
      ScheduledExecutorService executor,
      int schemaAgreementRetries,
      StreamObserver<EnrichedResponse> responseObserver,
      ExceptionHandler exceptionHandler) {
    super(query, connection, persistence, executor, schemaAgreementRetries, exceptionHandler);
    this.responseObserver = responseObserver;
  }

  @Override
  protected void setSuccess(EnrichedResponse response) {
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }
}
