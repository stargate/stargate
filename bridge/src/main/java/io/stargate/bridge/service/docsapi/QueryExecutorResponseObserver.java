package io.stargate.bridge.service.docsapi;

import io.grpc.stub.StreamObserver;
import io.stargate.proto.QueryOuterClass;
import java.util.concurrent.CompletableFuture;

public class QueryExecutorResponseObserver implements StreamObserver<QueryOuterClass.Response> {
  CompletableFuture<QueryOuterClass.Response> responseFuture;

  public QueryExecutorResponseObserver(CompletableFuture<QueryOuterClass.Response> responseFuture) {
    this.responseFuture = responseFuture;
  }

  @Override
  public void onNext(QueryOuterClass.Response response) {
    responseFuture.complete(response);
  }

  @Override
  public void onError(Throwable throwable) {
    responseFuture.completeExceptionally(throwable);
  }

  @Override
  public void onCompleted() {
    // intentionally empty
  }
}
