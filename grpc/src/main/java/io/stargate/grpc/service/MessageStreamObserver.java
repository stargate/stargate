package io.stargate.grpc.service;

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.stub.StreamObserver;
import io.stargate.proto.QueryOuterClass;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;

/**
 * Implements the {@link StreamObserver} interface. It is able to process Query and Batch (both
 * extends {@link GeneratedMessageV3}).
 *
 * @param <MessageT> - type of the Message to process. We support {@link
 *     io.stargate.proto.QueryOuterClass.Query} and {@link io.stargate.proto.QueryOuterClass.Batch}
 */
public class MessageStreamObserver<MessageT extends GeneratedMessageV3>
    implements StreamObserver<MessageT>, SuccessHandler {

  private final AtomicLong inFlight = new AtomicLong(0);
  private final AtomicBoolean clientSignalComplete = new AtomicBoolean(false);
  private final ExceptionHandler exceptionHandler;
  private final BiFunction<MessageT, SuccessHandler, MessageHandler<MessageT, ?>> handlerProducer;
  private final StreamObserver<QueryOuterClass.Response> responseObserver;

  public MessageStreamObserver(
      StreamObserver<QueryOuterClass.Response> responseObserver,
      ExceptionHandler exceptionHandler,
      BiFunction<MessageT, SuccessHandler, MessageHandler<MessageT, ?>> handlerProducer) {
    this.responseObserver = responseObserver;
    this.exceptionHandler = exceptionHandler;
    this.handlerProducer = handlerProducer;
  }

  /**
   * Calls {@code StreamObserver#onNext} for a response. Next, it decrements the {@code inFlight}
   * and checks if the {@code this#onCompleted()} was called. If it was (and there were no errors),
   * it calls {@code onCompleted()} on the {@code this#responseObserver}.
   *
   * @param response
   */
  @Override
  public void handleResponse(QueryOuterClass.Response response) {
    try {
      responseObserver.onNext(response);
    } finally {
      if (inFlight.decrementAndGet() == 0) {
        if (clientSignalComplete.get() && !exceptionHandler.getExceptionOccurred()) {
          responseObserver.onCompleted();
        }
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
    handlerProducer.apply(value, this).handle();
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
    if (exceptionHandler.getExceptionOccurred()) {
      return;
    }
    clientSignalComplete.set(true);

    if (inFlight.get() == 0) {
      responseObserver.onCompleted();
    }
  }
}
