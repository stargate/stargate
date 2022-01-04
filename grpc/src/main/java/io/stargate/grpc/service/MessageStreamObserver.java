package io.stargate.grpc.service;

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.stargate.proto.QueryOuterClass;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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

  static final int DELAY_BETWEEN_ON_COMPLETE_CHECKS_MS = 100;
  private static final int ON_COMPLETE_RETRIES_DEFAULT =
      Integer.getInteger("stargate.grpc.streaming.on_complete.retries", 100);

  private final AtomicLong inFlight = new AtomicLong(0);
  private final ExceptionHandler exceptionHandler;
  private final BiFunction<MessageT, AtomicLong, MessageHandler<MessageT, ?>> handlerProducer;
  private final StreamObserver<QueryOuterClass.Response> responseObserver;
  private final ScheduledExecutorService executor;
  private final int onCompleteRetries;

  MessageStreamObserver(
      StreamObserver<QueryOuterClass.Response> responseObserver,
      ExceptionHandler exceptionHandler,
      BiFunction<MessageT, AtomicLong, MessageHandler<MessageT, ?>> handlerProducer,
      ScheduledExecutorService executor,
      int onCompleteRetries) {
    this.responseObserver = responseObserver;
    this.exceptionHandler = exceptionHandler;
    this.handlerProducer = handlerProducer;
    this.executor = executor;
    this.onCompleteRetries = onCompleteRetries;
  }

  public MessageStreamObserver(
      StreamObserver<QueryOuterClass.Response> responseObserver,
      ExceptionHandler exceptionHandler,
      BiFunction<MessageT, AtomicLong, MessageHandler<MessageT, ?>> handlerProducer,
      ScheduledExecutorService executor) {
    this(
        responseObserver, exceptionHandler, handlerProducer, executor, ON_COMPLETE_RETRIES_DEFAULT);
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
    handlerProducer.apply(value, inFlight).handle();
  }

  @Override
  public void onError(Throwable t) {
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
    CompletableFuture<Void> shouldComplete = new CompletableFuture<>();
    if (exceptionHandler.getExceptionOccurred()) {
      return;
    }

    executor.schedule(
        () -> waitForAllInFlightProcessed(onCompleteRetries - 1, shouldComplete),
        DELAY_BETWEEN_ON_COMPLETE_CHECKS_MS,
        TimeUnit.MILLISECONDS);
    try {
      shouldComplete.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  private void waitForAllInFlightProcessed(
      int remainingAttempts, CompletableFuture<Void> shouldComplete) {
    if (exceptionHandler.getExceptionOccurred()) {
      shouldComplete.complete(null);
      return;
    }

    if (remainingAttempts == 0) {
      responseObserver.onError(
          Status.DEADLINE_EXCEEDED
              .withDescription(
                  "Failed to complete stream after "
                      + (DELAY_BETWEEN_ON_COMPLETE_CHECKS_MS * onCompleteRetries)
                      + " milliseconds.")
              .asException());
      shouldComplete.complete(null);
      return;
    }

    if (inFlight.get() > 0) {
      executor.schedule(
          () -> waitForAllInFlightProcessed(remainingAttempts - 1, shouldComplete),
          100,
          TimeUnit.MILLISECONDS);
    } else {
      responseObserver.onCompleted();
      shouldComplete.complete(null);
    }
  }
}
