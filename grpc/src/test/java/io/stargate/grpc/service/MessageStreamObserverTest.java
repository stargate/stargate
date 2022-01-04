package io.stargate.grpc.service;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import io.stargate.proto.QueryOuterClass;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class MessageStreamObserverTest {
  @Test
  @SuppressWarnings("unchecked")
  public void shouldOnCompleteWhileThereIsNoInFlight() {
    // given
    StreamObserver<QueryOuterClass.Response> callerStreamObserver = mock(StreamObserver.class);
    StreamObserver<QueryOuterClass.Response> responseStreamObserver =
        new SynchronizedStreamObserver<>(callerStreamObserver);
    ExceptionHandler exceptionHandler = new ExceptionHandler(responseStreamObserver);
    BiFunction<QueryOuterClass.Response, AtomicLong, MessageHandler<QueryOuterClass.Response, ?>>
        handlerProducer =
            (response, inFlight) -> {
              inFlight.decrementAndGet(); // inFlight is decremented
              return mock(MessageHandler.class);
            };
    ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

    MessageStreamObserver<QueryOuterClass.Response> observer =
        new MessageStreamObserver<>(
            responseStreamObserver, exceptionHandler, handlerProducer, executor, 10);

    // when
    observer.onNext(QueryOuterClass.Response.newBuilder().build());

    // then should immediately complete and call the caller onComplete
    assertDoesNotThrow(observer::onCompleted);
    verify(callerStreamObserver, times(1)).onCompleted();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldWaitWithOnCompleteWhileThereAreRequestsInFlight() {
    // given
    ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
    StreamObserver<QueryOuterClass.Response> callerStreamObserver = mock(StreamObserver.class);
    StreamObserver<QueryOuterClass.Response> responseStreamObserver =
        new SynchronizedStreamObserver<>(callerStreamObserver);
    ExceptionHandler exceptionHandler = new ExceptionHandler(responseStreamObserver);
    BiFunction<QueryOuterClass.Response, AtomicLong, MessageHandler<QueryOuterClass.Response, ?>>
        handlerProducer =
            (response, inFlight) -> {
              // decrement inFlight after a delay == 4 * delayBetweenOnCompleteChecks
              executor.schedule(
                  inFlight::decrementAndGet,
                  MessageStreamObserver.DELAY_BETWEEN_ON_COMPLETE_CHECKS_MS * 4,
                  TimeUnit.MILLISECONDS);
              return mock(MessageHandler.class);
            };

    MessageStreamObserver<QueryOuterClass.Response> observer =
        new MessageStreamObserver<>(
            responseStreamObserver, exceptionHandler, handlerProducer, executor, 10);

    // when
    observer.onNext(QueryOuterClass.Response.newBuilder().build());

    // then wait for inFlight = 0 and complete
    assertDoesNotThrow(
        observer
            ::onCompleted); // onCompleted will not return immediately because inFlight is not == 0
    verify(callerStreamObserver, times(1)).onCompleted();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldErrorFromOnCompleteWhileThereAreRequestsInFlightNotDecremented() {
    // given
    int limitOfRetries = 10;
    ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
    StreamObserver<QueryOuterClass.Response> callerStreamObserver = mock(StreamObserver.class);
    StreamObserver<QueryOuterClass.Response> responseStreamObserver =
        new SynchronizedStreamObserver<>(callerStreamObserver);
    ExceptionHandler exceptionHandler = new ExceptionHandler(responseStreamObserver);
    BiFunction<QueryOuterClass.Response, AtomicLong, MessageHandler<QueryOuterClass.Response, ?>>
        handlerProducer =
            (response, inFlight) -> {
              // do not decrement inFlight
              return mock(MessageHandler.class);
            };

    MessageStreamObserver<QueryOuterClass.Response> observer =
        new MessageStreamObserver<>(
            responseStreamObserver, exceptionHandler, handlerProducer, executor, limitOfRetries);

    // when
    observer.onNext(QueryOuterClass.Response.newBuilder().build());

    // then wait for limitOfRetries * MessageStreamObserver.DELAY_BETWEEN_ON_COMPLETE_CHECKS_MS and
    // invoke onError
    assertDoesNotThrow(observer::onCompleted);
    ArgumentCaptor<Throwable> errorCapture = ArgumentCaptor.forClass(Throwable.class);
    verify(callerStreamObserver, times(1)).onError(errorCapture.capture());
    assertThat(errorCapture.getValue()).isInstanceOf(StatusException.class);
    assertThat(errorCapture.getValue())
        .hasMessageContaining(
            "Failed to complete stream after "
                + (MessageStreamObserver.DELAY_BETWEEN_ON_COMPLETE_CHECKS_MS * limitOfRetries)
                + " milliseconds.");
  }
}
