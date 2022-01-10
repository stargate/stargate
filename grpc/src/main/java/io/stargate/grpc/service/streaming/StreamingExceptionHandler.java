package io.stargate.grpc.service.streaming;

import com.google.protobuf.Any;
import com.google.rpc.ErrorInfo;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.stargate.grpc.service.ExceptionHandler;
import io.stargate.grpc.service.StreamingSuccessHandler;
import io.stargate.proto.QueryOuterClass;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class StreamingExceptionHandler extends ExceptionHandler {
  private final StreamingSuccessHandler streamingSuccessHandler;

  public StreamingExceptionHandler(StreamingSuccessHandler streamingSuccessHandler) {
    this.streamingSuccessHandler = streamingSuccessHandler;
  }

  @Override
  protected void onError(
      @Nullable Status status, @Nonnull Throwable throwable, @Nullable Metadata trailer) {
    // propagate streaming error as a Status
    streamingSuccessHandler.handleResponse(
        QueryOuterClass.StreamingResponse.newBuilder()
            .setStatus(convertStatus(status, throwable))
            .build());
  }

  private com.google.rpc.Status convertStatus(
      @Nullable Status status, @Nonnull Throwable throwable) {
    if (status == null) {
      if (throwable instanceof StatusException) {
        status = ((StatusException) throwable).getStatus();
      } else if (throwable instanceof StatusRuntimeException) {
        status = ((StatusRuntimeException) throwable).getStatus();
      } else {
        status = Status.UNKNOWN;
      }
    }

    String code = status.getCode().toString();
    String cause = Optional.ofNullable(throwable.getMessage()).orElse("");

    return com.google.rpc.Status.newBuilder()
        .setCode(status.getCode().value())
        .setMessage(String.format("%s: %s", code, cause))
        .addDetails(Any.pack(ErrorInfo.newBuilder().setReason(cause).build()))
        .build();
  }
}
