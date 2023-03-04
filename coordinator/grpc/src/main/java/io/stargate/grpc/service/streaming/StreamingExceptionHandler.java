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

/**
 * This exception handler is used for all bi-streaming operations. In case an error occurred, it
 * converts it to the {@link com.google.rpc.Status} and put it in the {@link
 * QueryOuterClass.StreamingResponse}.
 */
public class StreamingExceptionHandler extends ExceptionHandler {
  private final StreamingSuccessHandler streamingSuccessHandler;

  public StreamingExceptionHandler(StreamingSuccessHandler streamingSuccessHandler) {
    this.streamingSuccessHandler = streamingSuccessHandler;
  }

  /**
   * It converts status and throwable to the {@link
   * io.stargate.proto.QueryOuterClass.StreamingResponse}.
   *
   * @param status - the error status of the call. It can be null.
   * @param throwable - the throwable that caused the error. It cannot be null.
   * @param trailer - the metadata associated with the error. It can be null.
   */
  @Override
  protected void onError(
      @Nullable Status status, @Nonnull Throwable throwable, @Nullable Metadata trailer) {
    // propagate streaming error as a Status
    streamingSuccessHandler.handleResponse(
        QueryOuterClass.StreamingResponse.newBuilder()
            .setStatus(convertStatus(status, throwable))
            .build());
  }

  /**
   * It converts the throwable and status to the {@link com.google.rpc.Status}. If the status is
   * null and throwable is a {@link StatusException} or {@link StatusRuntimeException} it extracts
   * its status. If the status is null and throwable is another instance type, it will have the
   * status code {@code Status.UNKNOWN}.
   *
   * <p>The constructed status will have the status code and a message containing the code's string
   * representation with the cause message - if it is not null.
   *
   * @param status - the error status of the call. It can be null.
   * @param throwable - the throwable that caused the error. It cannot be null.
   * @return the com.google.rpc.Status that can be put directly into the {@link
   *     io.stargate.proto.QueryOuterClass.StreamingResponse}.
   */
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
