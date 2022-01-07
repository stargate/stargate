package io.stargate.grpc.service;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.stargate.proto.QueryOuterClass;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class SingleExceptionHandler extends ExceptionHandler {

  private final StreamObserver<QueryOuterClass.Response> responseObserver;

  public SingleExceptionHandler(StreamObserver<QueryOuterClass.Response> responseObserver) {
    this.responseObserver = responseObserver;
  }

  @Override
  protected void onError(
      @Nonnull Status status, @Nonnull Throwable throwable, @Nullable Metadata trailer) {
    status = status.withDescription(throwable.getMessage()).withCause(throwable);
    responseObserver.onError(
        trailer != null ? status.asRuntimeException(trailer) : status.asRuntimeException());
  }
}
