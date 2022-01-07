package io.stargate.grpc.service.streaming;

import com.google.protobuf.Any;
import com.google.rpc.ErrorInfo;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.stargate.grpc.service.ExceptionHandler;
import io.stargate.grpc.service.SuccessHandler;
import io.stargate.proto.QueryOuterClass;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class StreamingExceptionHandler extends ExceptionHandler {
  private final SuccessHandler successHandler;

  public StreamingExceptionHandler(SuccessHandler successHandler) {
    this.successHandler = successHandler;
  }

  @Override
  protected void onError(
      @Nullable Status status, @Nonnull Throwable throwable, @Nullable Metadata trailer) {
    // propagate streaming error as a Status
    successHandler.handleResponse(
        QueryOuterClass.StreamingResponse.newBuilder()
            .setStatus(convertStatus(status, throwable, trailer))
            .build());
  }

  private com.google.rpc.Status convertStatus(
      @Nullable Status status, @Nonnull Throwable throwable, @Nullable Metadata trailer) {
    if (status == null) {
      if (throwable instanceof StatusException) {
        status = ((StatusException) throwable).getStatus();
      } else if (throwable instanceof StatusRuntimeException) {
        status = ((StatusRuntimeException) throwable).getStatus();
      } else {
        status = Status.UNKNOWN;
      }
    }

    String description = status.getCode().toString();
    String cause = Optional.ofNullable(throwable.getMessage()).orElse("");

    Map<String, String> metadata = new HashMap<>();
    if (trailer != null) {
      for (String key : trailer.keys()) {
        //            trailer.get(Metadata.Key.of(key))
        // todo iterate over metadata and put it into Map
      }
    }
    return com.google.rpc.Status.newBuilder()
        .setCode(status.getCode().value())
        .setMessage(description)
        .addDetails(
            Any.pack(ErrorInfo.newBuilder().setReason(cause).putAllMetadata(metadata).build()))
        .build();
  }
}
