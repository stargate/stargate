package io.stargate.grpc.service;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.stargate.proto.BridgeQuery.EnrichedResponse;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class SingleEnrichedExceptionHandler extends ExceptionHandler {

  private final StreamObserver<EnrichedResponse> responseObserver;

  public SingleEnrichedExceptionHandler(StreamObserver<EnrichedResponse> responseObserver) {
    this.responseObserver = responseObserver;
  }

  @Override
  protected void onError(
      @Nullable Status status, @Nonnull Throwable throwable, @Nullable Metadata trailer) {
    if (status == null) {
      responseObserver.onError(throwable);
      return;
    }
    status = status.withDescription(throwable.getMessage()).withCause(throwable);
    responseObserver.onError(
        trailer != null ? status.asRuntimeException(trailer) : status.asRuntimeException());
  }
}
