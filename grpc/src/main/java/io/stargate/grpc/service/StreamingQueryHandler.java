package io.stargate.grpc.service;

import io.grpc.stub.StreamObserver;
import io.stargate.db.Persistence;
import io.stargate.proto.QueryOuterClass;
import java.util.concurrent.ScheduledExecutorService;

public class StreamingQueryHandler extends QueryHandler {
  StreamingQueryHandler(
      QueryOuterClass.Query query,
      Persistence.Connection connection,
      Persistence persistence,
      ScheduledExecutorService executor,
      int schemaAgreementRetries,
      StreamObserver<QueryOuterClass.Response> responseObserver) {
    super(query, connection, persistence, executor, schemaAgreementRetries, responseObserver);
  }

  @Override
  protected void setSuccess(QueryOuterClass.Response response) {
    responseObserver.onNext(response);
    // do not invoke onComplete. The caller(client) may invoke it once it completes sending a stream
    // of
    // queries
  }
}
