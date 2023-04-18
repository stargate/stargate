package io.stargate.sgv2.api.common.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableMap;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.grpc.qualifier.Retriable;
import io.stargate.sgv2.common.bridge.BridgeTest;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import java.util.Map;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(RetriableStargateBridgeDisabledTest.Profile.class)
class RetriableStargateBridgeDisabledTest extends BridgeTest {

  public static class Profile implements NoGlobalResourcesTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .put("stargate.grpc.retries.enabled", "false")
          .put("stargate.grpc.retries.status-codes", "UNAVAILABLE,NOT_FOUND")
          .build();
    }
  }

  @Retriable @Inject RetriableStargateBridge bridge;

  @Test
  public void disabledNoRetries() {
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

    // verify one class only
    verify(bridgeService).executeQuery(eq(request), any());
  }
}
