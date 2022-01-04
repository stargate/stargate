package io.stargate.grpc.service;

import io.grpc.stub.StreamObserver;
import io.stargate.db.Persistence;
import io.stargate.proto.QueryOuterClass;
import java.util.concurrent.atomic.AtomicLong;

public class StreamingBatchHandler extends BatchHandler {
  private final AtomicLong inFlight;

  StreamingBatchHandler(
      QueryOuterClass.Batch batch,
      Persistence.Connection connection,
      Persistence persistence,
      StreamObserver<QueryOuterClass.Response> responseObserver,
      AtomicLong inFlight,
      ExceptionHandler exceptionHandler) {
    super(batch, connection, persistence, responseObserver, exceptionHandler);
    this.inFlight = inFlight;
  }

  @Override
  protected void setSuccess(QueryOuterClass.Response response) {
    System.out.println(
        "SetSuccess for Batch: " + response + "Thread: " + Thread.currentThread().getName());
    responseObserver.onNext(response);
    inFlight.decrementAndGet();
    // do not invoke onComplete. The caller(client) may invoke it
    // once it completes sending a stream of queries
  }
}
