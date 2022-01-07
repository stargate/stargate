package io.stargate.grpc.service.streaming;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.protobuf.Any;
import com.google.rpc.ErrorInfo;
import io.grpc.Status;
import io.grpc.StatusException;
import io.stargate.grpc.service.SuccessHandler;
import io.stargate.proto.QueryOuterClass;
import java.util.stream.Stream;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.apache.cassandra.stargate.db.WriteType;
import org.apache.cassandra.stargate.exceptions.UnhandledClientException;
import org.apache.cassandra.stargate.exceptions.WriteTimeoutException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class StreamingExceptionHandlerTest {

  @ParameterizedTest
  @MethodSource("exceptions")
  public void shouldMapErrorToStreamingResponse(
      Throwable throwable,
      Status expectedStatus,
      String expectedDescription,
      String expectedCause) {
    // given
    SuccessHandler successHandler = mock(SuccessHandler.class);
    StreamingExceptionHandler streamingExceptionHandler =
        new StreamingExceptionHandler(successHandler);

    // when
    streamingExceptionHandler.handleException(throwable);

    // then
    verify(successHandler, times(1))
        .handleResponse(
            QueryOuterClass.StreamingResponse.newBuilder()
                .setStatus(
                    com.google.rpc.Status.newBuilder()
                        .setCode(expectedStatus.getCode().value())
                        .setMessage(expectedDescription)
                        .addDetails(
                            Any.pack(ErrorInfo.newBuilder().setReason(expectedCause).build()))
                        .build())
                .build());
  }

  public static Stream<Arguments> exceptions() {
    return Stream.of(
        Arguments.of(
            new StatusException(Status.UNAVAILABLE),
            Status.UNAVAILABLE,
            "UNAVAILABLE",
            "UNAVAILABLE"),
        Arguments.of(
            new UnhandledClientException("Problem when handling"),
            Status.UNAVAILABLE,
            "UNAVAILABLE",
            "Problem when handling"),
        Arguments.of(
            new WriteTimeoutException(WriteType.SIMPLE, ConsistencyLevel.ALL, 0, 0),
            Status.DEADLINE_EXCEEDED,
            "DEADLINE_EXCEEDED",
            "Operation timed out - received only 0 responses."));
  }
}
