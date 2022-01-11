package io.stargate.grpc.service.streaming;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.grpc.stub.StreamObserver;
import io.stargate.grpc.service.ExceptionHandler;
import io.stargate.grpc.service.MessageHandler;
import io.stargate.grpc.service.SynchronizedStreamObserver;
import io.stargate.proto.QueryOuterClass;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

class MessageStreamObserverTest {
  private static final ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(1);

  @Test
  @SuppressWarnings("unchecked")
  public void shouldOnCompleteWhenThereIsNoInFlight() {
    // given
    StreamObserver<QueryOuterClass.StreamingResponse> callerStreamObserver =
        mock(StreamObserver.class);
    StreamObserver<QueryOuterClass.StreamingResponse> responseStreamObserver =
        new SynchronizedStreamObserver<>(callerStreamObserver);
    StreamingHandlerFactory<QueryOuterClass.Query> streamingHandlerFactory =
        (query, successHandler, exH) -> {
          successHandler.handleResponse(
              QueryOuterClass.StreamingResponse.newBuilder().build()); // inFlight is decremented
          return mock(MessageHandler.class);
        };

    MessageStreamObserver<QueryOuterClass.Query> observer =
        new MessageStreamObserver<QueryOuterClass.Query>(
            responseStreamObserver, (v) -> mock(ExceptionHandler.class), streamingHandlerFactory);

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
    StreamObserver<QueryOuterClass.StreamingResponse> callerStreamObserver =
        mock(StreamObserver.class);
    StreamObserver<QueryOuterClass.StreamingResponse> responseStreamObserver =
        new SynchronizedStreamObserver<>(callerStreamObserver);
    StreamingHandlerFactory<QueryOuterClass.Query> streamingHandlerFactory =
        (query, successHandler, exH) -> {
          // decrement inFlight after a delay 500 MS delay
          executor.schedule(
              () ->
                  successHandler.handleResponse(
                      QueryOuterClass.StreamingResponse.newBuilder().build()),
              500,
              TimeUnit.MILLISECONDS);
          return mock(MessageHandler.class);
        };

    MessageStreamObserver<QueryOuterClass.Query> observer =
        new MessageStreamObserver<QueryOuterClass.Query>(
            responseStreamObserver, (v) -> mock(ExceptionHandler.class), streamingHandlerFactory);

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
    StreamObserver<QueryOuterClass.StreamingResponse> callerStreamObserver =
        mock(StreamObserver.class);
    StreamObserver<QueryOuterClass.StreamingResponse> responseStreamObserver =
        new SynchronizedStreamObserver<>(callerStreamObserver);
    StreamingHandlerFactory<QueryOuterClass.Query> streamingHandlerFactory =
        (query, successHandler, exH) -> {
          // do not decrement inFlight
          return mock(MessageHandler.class);
        };

    MessageStreamObserver<QueryOuterClass.Query> observer =
        new MessageStreamObserver<QueryOuterClass.Query>(
            responseStreamObserver, (v) -> mock(ExceptionHandler.class), streamingHandlerFactory);

    // when
    observer.onNext(QueryOuterClass.Query.newBuilder().build());
    observer.onCompleted();
    // then should not call the caller' onComplete because there are still requests in-flight
    verify(callerStreamObserver, timeout(1000).times(0)).onCompleted();
  }

  @AfterAll
  public static void cleanup() {
    EXECUTOR.shutdown();
  }
}
