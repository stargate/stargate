package io.stargate.grpc.service.streaming;

import com.google.protobuf.GeneratedMessageV3;
import io.stargate.grpc.service.ExceptionHandler;
import io.stargate.grpc.service.MessageHandler;
import io.stargate.grpc.service.StreamingSuccessHandler;

public interface StreamingHandlerFactory<MessageT extends GeneratedMessageV3> {
  MessageHandler<MessageT, ?> create(
      MessageT messageT,
      StreamingSuccessHandler streamingSuccessHandler,
      ExceptionHandler exceptionHandler);
}
