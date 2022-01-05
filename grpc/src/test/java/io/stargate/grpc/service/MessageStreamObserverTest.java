package io.stargate.grpc.service;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.grpc.stub.StreamObserver;
import io.stargate.proto.QueryOuterClass;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import org.junit.jupiter.api.Test;

class MessageStreamObserverTest {
  @Test
  @SuppressWarnings("unchecked")
  public void shouldOnCompleteWhenThereIsNoInFlight() {
    // given
    StreamObserver<QueryOuterClass.Response> callerStreamObserver = mock(StreamObserver.class);
    StreamObserver<QueryOuterClass.Response> responseStreamObserver =
        new SynchronizedStreamObserver<>(callerStreamObserver);
    ExceptionHandler exceptionHandler = new ExceptionHandler(responseStreamObserver);
    BiFunction<QueryOuterClass.Query, SuccessHandler, MessageHandler<QueryOuterClass.Query, ?>>
        handlerProducer =
            (query, successHandler) -> {
              successHandler.handleResponse(
                  QueryOuterClass.Response.newBuilder().build()); // inFlight is decremented
              return mock(MessageHandler.class);
            };

    MessageStreamObserver<QueryOuterClass.Query> observer =
        new MessageStreamObserver<QueryOuterClass.Query>(
            responseStreamObserver, exceptionHandler, handlerProducer);

    // when
    observer.onNext(QueryOuterClass.Query.newBuilder().build());
    // then should immediately complete and call the caller' onComplete
    observer.onCompleted();
    verify(callerStreamObserver, times(1)).onCompleted();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldWaitWithOnCompleteWhenThereAreRequestsInFlight() {
    // given
    ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
    StreamObserver<QueryOuterClass.Response> callerStreamObserver = mock(StreamObserver.class);
    StreamObserver<QueryOuterClass.Response> responseStreamObserver =
        new SynchronizedStreamObserver<>(callerStreamObserver);
    ExceptionHandler exceptionHandler = new ExceptionHandler(responseStreamObserver);
    BiFunction<QueryOuterClass.Query, SuccessHandler, MessageHandler<QueryOuterClass.Query, ?>>
        handlerProducer =
            (query, successHandler) -> {
              // decrement inFlight after a delay 500 MS delay
              executor.schedule(
                  () ->
                      successHandler.handleResponse(QueryOuterClass.Response.newBuilder().build()),
                  500,
                  TimeUnit.MILLISECONDS);
              return mock(MessageHandler.class);
            };

    MessageStreamObserver<QueryOuterClass.Query> observer =
        new MessageStreamObserver<QueryOuterClass.Query>(
            responseStreamObserver, exceptionHandler, handlerProducer);

    // when
    observer.onNext(QueryOuterClass.Query.newBuilder().build());

    // then wait for inFlight = 0 and complete
    observer
        .onCompleted(); // onCompleted will not trigger immediate callerStreamObserver#onCompleted
    // because inFlight is not == 0
    verify(callerStreamObserver, timeout(1000).times(1)).onCompleted();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldNotCallOnCompleteWhenThereAreRequestsInFlightNotDecremented() {
    // given
    StreamObserver<QueryOuterClass.Response> callerStreamObserver = mock(StreamObserver.class);
    StreamObserver<QueryOuterClass.Response> responseStreamObserver =
        new SynchronizedStreamObserver<>(callerStreamObserver);
    ExceptionHandler exceptionHandler = new ExceptionHandler(responseStreamObserver);
    BiFunction<QueryOuterClass.Query, SuccessHandler, MessageHandler<QueryOuterClass.Query, ?>>
        handlerProducer =
            (query, successHandler) -> {
              // do not decrement inFlight
              return mock(MessageHandler.class);
            };

    MessageStreamObserver<QueryOuterClass.Query> observer =
        new MessageStreamObserver<QueryOuterClass.Query>(
            responseStreamObserver, exceptionHandler, handlerProducer);

    // when
    observer.onNext(QueryOuterClass.Query.newBuilder().build());
    observer.onCompleted();
    // then should not call the caller' onComplete because there are still requests in-flight
    verify(callerStreamObserver, timeout(1000).times(0)).onCompleted();
  }
}
