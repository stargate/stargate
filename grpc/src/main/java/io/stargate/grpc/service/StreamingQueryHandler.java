package io.stargate.grpc.service;

import io.grpc.stub.StreamObserver;
import io.stargate.db.Persistence;
import io.stargate.proto.QueryOuterClass;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public class StreamingQueryHandler extends QueryHandler {
  private final AtomicLong inFlight;

  StreamingQueryHandler(
      QueryOuterClass.Query query,
      Persistence.Connection connection,
      Persistence persistence,
      ScheduledExecutorService executor,
      int schemaAgreementRetries,
      StreamObserver<QueryOuterClass.Response> responseObserver,
      AtomicLong inFlight,
      ExceptionHandler exceptionHandler) {
    super(
        query,
        connection,
        persistence,
        executor,
        schemaAgreementRetries,
        responseObserver,
        exceptionHandler);
    this.inFlight = inFlight;
  }

  @Override
  protected synchronized void setSuccess(QueryOuterClass.Response response) {
    responseObserver.onNext(response);
    inFlight.decrementAndGet();
    // do not invoke onComplete. The caller(client) may invoke it
    // once it completes sending a stream of queries
  }
}
