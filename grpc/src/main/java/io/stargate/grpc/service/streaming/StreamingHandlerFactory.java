package io.stargate.grpc.service.streaming;

import com.google.protobuf.GeneratedMessageV3;
import io.stargate.grpc.service.ExceptionHandler;
import io.stargate.grpc.service.MessageHandler;
import io.stargate.grpc.service.SuccessHandler;

public interface StreamingHandlerFactory<MessageT extends GeneratedMessageV3> {
  MessageHandler<MessageT, ?> create(
      MessageT messageT, SuccessHandler successHandler, ExceptionHandler exceptionHandler);
}
