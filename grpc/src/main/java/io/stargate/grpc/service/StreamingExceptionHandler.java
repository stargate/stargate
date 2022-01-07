package io.stargate.grpc.service;

import com.google.protobuf.Any;
import com.google.rpc.ErrorInfo;
import io.grpc.Metadata;
import io.grpc.Status;
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
      @Nonnull Status status, @Nonnull Throwable throwable, @Nullable Metadata trailer) {
    status = status.withDescription(throwable.getMessage()).withCause(throwable);
    // propagate streaming error as a Status
    successHandler.handleResponse(
        QueryOuterClass.StreamingResponse.newBuilder()
            .setStatus(convertStatus(status, trailer))
            .build());
  }

  private com.google.rpc.Status convertStatus(Status status, @Nullable Metadata trailer) {
    String description = Optional.ofNullable(status.getDescription()).orElse("");
    String cause = Optional.ofNullable(status.getCause()).map(Throwable::getMessage).orElse("");

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
