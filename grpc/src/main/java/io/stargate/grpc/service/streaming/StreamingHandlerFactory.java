package io.stargate.grpc.service.streaming;

import com.google.protobuf.GeneratedMessageV3;
import io.stargate.grpc.service.ExceptionHandler;
import io.stargate.grpc.service.MessageHandler;
import io.stargate.grpc.service.StreamingSuccessHandler;

/**
 * It creates an instance of a {@link MessageHandler} for a given message, the {@link
 * StreamingSuccessHandler} and the {@link ExceptionHandler}.
 *
 * @param <MessageT>
 */
public interface StreamingHandlerFactory<MessageT extends GeneratedMessageV3> {
  MessageHandler<MessageT, ?> create(
      MessageT messageT,
      StreamingSuccessHandler streamingSuccessHandler,
      ExceptionHandler exceptionHandler);
}
