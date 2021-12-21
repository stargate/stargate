package io.stargate.grpc.service;

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.stub.StreamObserver;
import io.stargate.proto.QueryOuterClass;
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
    implements StreamObserver<MessageT> {

  private final AtomicLong inFlight = new AtomicLong(0);
  private final ExceptionHandler exceptionHandler;
  private final BiFunction<MessageT, AtomicLong, MessageHandler<MessageT, ?>> handlerProducer;
  private final StreamObserver<QueryOuterClass.Response> responseObserver;

  public MessageStreamObserver(
      StreamObserver<QueryOuterClass.Response> responseObserver,
      ExceptionHandler exceptionHandler,
      BiFunction<MessageT, AtomicLong, MessageHandler<MessageT, ?>> handlerProducer) {
    this.responseObserver = responseObserver;
    this.exceptionHandler = exceptionHandler;
    this.handlerProducer = handlerProducer;
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
    System.out.println(
        "Server side onNext for: " + value + " thread: " + Thread.currentThread().getName());
    handlerProducer.apply(value, inFlight).handle();
  }

  @Override
  public void onError(Throwable t) {
    System.out.println("onError for reactive streaming: " + t);
    exceptionHandler.handleException(t);
  }

  /**
   * It completes the response Observer. If there are any in-flight Queries, it waits until all of
   * them are processed.
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

    System.out.println(
        "onCompleted server side "
            + " thread: "
            + Thread.currentThread().getName()
            + " inFlight: "
            + inFlight.get());
    while (inFlight.get() > 0) {
      if (exceptionHandler.getExceptionOccurred()) {
        return;
      }
      // todo do we want to wait until in-flight is empty?
      System.out.println("waiting while inFlight = 0");
    }
    responseObserver.onCompleted();
  }
}
