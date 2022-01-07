package io.stargate.grpc.service;

import com.google.protobuf.GeneratedMessageV3;

public interface StreamingHandlerFactory<MessageT extends GeneratedMessageV3> {
  MessageHandler<MessageT, ?> create(
      MessageT messageT, SuccessHandler successHandler, ExceptionHandler exceptionHandler);
}
