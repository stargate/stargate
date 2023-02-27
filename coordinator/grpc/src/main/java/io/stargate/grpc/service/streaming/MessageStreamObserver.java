package io.stargate.grpc.service.streaming;

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.stub.StreamObserver;
import io.stargate.grpc.service.ExceptionHandler;
import io.stargate.grpc.service.MessageHandler;
import io.stargate.grpc.service.StreamingSuccessHandler;
import io.stargate.proto.QueryOuterClass;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Implements the {@link StreamObserver} interface. It is able to process Query and Batch (both
 * extends {@link GeneratedMessageV3}).
 *
 * @param <MessageT> - type of the Message to process. We support {@link
 *     io.stargate.proto.QueryOuterClass.Query} and {@link io.stargate.proto.QueryOuterClass.Batch}
 */
public class MessageStreamObserver<MessageT extends GeneratedMessageV3>
    implements StreamObserver<MessageT>, StreamingSuccessHandler {

  private final AtomicLong inFlight = new AtomicLong(0);
  private final AtomicBoolean clientSignalComplete = new AtomicBoolean(false);
  private final ExceptionHandler exceptionHandler;
  StreamingHandlerFactory<MessageT> streamingHandlerFactory;
  private final StreamObserver<QueryOuterClass.StreamingResponse> responseObserver;

  public MessageStreamObserver(
      StreamObserver<QueryOuterClass.StreamingResponse> responseObserver,
      Function<StreamingSuccessHandler, ExceptionHandler> exceptionHandlerProducer,
      StreamingHandlerFactory<MessageT> streamingHandlerFactory) {
    this.responseObserver = responseObserver;
    this.streamingHandlerFactory = streamingHandlerFactory;
    this.exceptionHandler = exceptionHandlerProducer.apply(this);
  }

  /**
   * Calls {@code StreamObserver#onNext} for a response. Next, it decrements the {@code inFlight}
   * and checks if the {@code this#onCompleted()} was called. If it was (and there were no errors),
   * it calls {@code onCompleted()} on the {@code this#responseObserver}.
   *
   * @param response
   */
  @Override
  public void handleResponse(QueryOuterClass.StreamingResponse response) {
    try {
      responseObserver.onNext(response);
    } finally {
      if (inFlight.decrementAndGet() == 0 && clientSignalComplete.get()) {
        responseObserver.onCompleted();
      }
    }
  }

  /**
   * It is creating new handler for each request and invokes the {@link MessageHandler#handle()}
   * method.
   *
   * @param value Batch or Query
   */
  @Override
  public void onNext(MessageT value) {
    inFlight.incrementAndGet();
    streamingHandlerFactory.create(value, this, exceptionHandler).handle();
  }

  @Override
  public void onError(Throwable t) {
    exceptionHandler.handleException(t);
  }

  /**
   * It completes the response Observer. If there are no in-flight Queries, it calls the onComplete
   * immediately(). If there are any in-flight queries, it will not call onComplete() but only save
   * the fact that the onComplete was issued by the caller. The last {@code
   * this#onNext(GeneratedMessageV3)} will call {@code StreamObserver#onCompleted()} when there will
   * be no more in-flight requests. For more info, see documentation of {@code
   * this#handleResponse(QueryOuterClass.Response)}.
   *
   * <p>According to reactive contract:
   *
   * <p>An Observable may make zero or more OnNext notifications, each representing a single emitted
   * item, and it may then follow those emission notifications by either an OnCompleted or an
   * OnError notification, but not both.
   *
   * <p>If there was any error (server-side or client-side) we are ignoring the onComplete() signal.
   */
  @Override
  public void onCompleted() {
    clientSignalComplete.set(true);

    if (inFlight.get() == 0) {
      responseObserver.onCompleted();
    }
  }
}
