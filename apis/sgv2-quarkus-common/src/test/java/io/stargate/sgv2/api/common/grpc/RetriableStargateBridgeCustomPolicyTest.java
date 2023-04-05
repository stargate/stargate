package io.stargate.sgv2.api.common.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableMap;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.quarkus.arc.lookup.LookupIfProperty;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.grpc.qualifier.Retriable;
import io.stargate.sgv2.api.common.grpc.retries.GrpcRetryPredicate;
import io.stargate.sgv2.common.bridge.BridgeTest;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import java.util.Map;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(RetriableStargateBridgeCustomPolicyTest.Profile.class)
class RetriableStargateBridgeCustomPolicyTest extends BridgeTest {

  public static class Profile implements NoGlobalResourcesTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .put("stargate.grpc.retries.policy", "custom")
          .put("stargate.grpc.retries.status-codes", "UNAVAILABLE,NOT_FOUND")
          .put("stargate.grpc.retries.max-attempts", "3")
          .build();
    }
  }

  public static final Metadata.Key<String> key =
      Metadata.Key.of("test-key", Metadata.ASCII_STRING_MARSHALLER);

  @Produces
  @ApplicationScoped
  @LookupIfProperty(name = "stargate.grpc.retries.policy", stringValue = "custom")
  GrpcRetryPredicate custom() {
    return e -> {
      Metadata trailers = e.getTrailers();
      return null != trailers && trailers.containsKey(key);
    };
  }

  @Retriable @Inject RetriableStargateBridge bridge;

  @Test
  public void retried() {
    doAnswer(
            invocationOnMock -> {
              StreamObserver<QueryOuterClass.Response> observer = invocationOnMock.getArgument(1);
              Status status = Status.UNAVAILABLE;
              Metadata trailer = new Metadata();
              trailer.put(key, "value");
              observer.onError(new StatusRuntimeException(status, trailer));
              return null;
            })
        .when(bridgeService)
        .executeQuery(any(), any());

    QueryOuterClass.Query request = QueryOuterClass.Query.newBuilder().build();
    Throwable result =
        bridge
            .executeQuery(request)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create())
            .awaitFailure()
            .getFailure();

    assertThat(result)
        .isInstanceOfSatisfying(
            StatusRuntimeException.class,
            e -> {
              assertThat(e.getStatus()).isEqualTo(Status.UNAVAILABLE);
              assertThat(e.getTrailers()).isNotNull();
              assertThat(e.getTrailers().get(key)).isEqualTo("value");
            });

    // verify 4 bridge calls, original + 3 retries
    // always same query
    verify(bridgeService, times(4)).executeQuery(eq(request), any());
  }

  @Test
  public void notRetried() {
    doAnswer(
            invocationOnMock -> {
              StreamObserver<QueryOuterClass.Response> observer = invocationOnMock.getArgument(1);
              Status status = Status.UNAVAILABLE;
              observer.onError(new StatusRuntimeException(status));
              return null;
            })
        .when(bridgeService)
        .executeQuery(any(), any());

    QueryOuterClass.Query request = QueryOuterClass.Query.newBuilder().build();
    Throwable result =
        bridge
            .executeQuery(request)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create())
            .awaitFailure()
            .getFailure();

    assertThat(result)
        .isInstanceOfSatisfying(
            StatusRuntimeException.class,
            e -> assertThat(e.getStatus()).isEqualTo(Status.UNAVAILABLE));

    // no retry
    verify(bridgeService).executeQuery(eq(request), any());
  }
}
