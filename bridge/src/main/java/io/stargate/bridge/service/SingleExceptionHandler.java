package io.stargate.bridge.service;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.stargate.bridge.proto.QueryOuterClass;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class SingleExceptionHandler extends ExceptionHandler {

  private final StreamObserver<QueryOuterClass.Response> responseObserver;

  public SingleExceptionHandler(StreamObserver<QueryOuterClass.Response> responseObserver) {
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
