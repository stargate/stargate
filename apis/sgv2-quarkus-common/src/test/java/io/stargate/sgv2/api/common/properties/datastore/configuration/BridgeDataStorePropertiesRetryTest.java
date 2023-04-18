package io.stargate.sgv2.api.common.properties.datastore.configuration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import io.stargate.bridge.proto.Schema;
import io.stargate.bridge.proto.StargateBridgeGrpc;
import io.stargate.sgv2.api.common.config.DataStoreConfig;
import io.stargate.sgv2.api.common.properties.datastore.DataStoreProperties;
import io.stargate.sgv2.common.bridge.BridgeTest;
import java.util.concurrent.atomic.AtomicInteger;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

/**
 * Tests for verifying retry logic of {@link DataStorePropertiesConfiguration#configuration} call.
 *
 * <p>NOTE: properties of {@code @Retry} annotation are overridden to both reduce delay between
 * calls AND to lower max-retry to 2 calls. See "./src/test/resources/application.yaml" for details.
 * Override logic by Quarkus/SmallRye is explained <a
 * href="https://quarkus.io/guides/smallrye-fault-tolerance">here</a>.
 */
@QuarkusTest
public class BridgeDataStorePropertiesRetryTest extends BridgeTest {
  @GrpcClient("bridge")
  StargateBridgeGrpc.StargateBridgeBlockingStub bridge;

  @Inject DataStorePropertiesConfiguration dataStorePropertiesConfiguration;

  private final Schema.SupportedFeaturesResponse SUCCESS_RESPONSE =
      Schema.SupportedFeaturesResponse.newBuilder()
          .setSecondaryIndexes(false)
          .setSai(false)
          .setLoggedBatches(true)
          .build();

  @Test
  public void dataStoreWithNoRetriesOk() {
    final AtomicInteger callCounter = new AtomicInteger(0);
    DataStoreProperties props = fetchDataStoreProperties(1, callCounter, SUCCESS_RESPONSE);
    verifyProperties(props, SUCCESS_RESPONSE);
    assertThat(callCounter).hasValue(1);
  }

  @Test
  public void dataStoreWithOneRetryOk() {
    final AtomicInteger callCounter = new AtomicInteger(0);
    DataStoreProperties props = fetchDataStoreProperties(2, callCounter, SUCCESS_RESPONSE);
    verifyProperties(props, SUCCESS_RESPONSE);
    assertThat(callCounter).hasValue(2);
  }

  @Test
  public void dataStoreWithTwoRetriesOk() {
    // Succeeds still with 2 retries (max for tests)
    final AtomicInteger callCounter = new AtomicInteger(0);
    DataStoreProperties props = fetchDataStoreProperties(3, callCounter, SUCCESS_RESPONSE);
    verifyProperties(props, SUCCESS_RESPONSE);
    assertThat(callCounter).hasValue(3);
  }

  @Test
  public void dataStoreWithTwoRetriesFail() {
    // Fails after 5 calls
    final AtomicInteger callCounter = new AtomicInteger(0);
    Throwable e = catchThrowable(() -> fetchDataStoreProperties(6, callCounter, SUCCESS_RESPONSE));
    assertThat(e).isNotNull().isInstanceOf(StatusRuntimeException.class);
    assertThat(e.getMessage()).contains("UNAVAILABLE");
    // and should have called 5 times before failing
    assertThat(callCounter).hasValue(5);
  }

  private void verifyProperties(
      DataStoreProperties props, Schema.SupportedFeaturesResponse expected) {
    assertThat(props.secondaryIndexesEnabled()).isEqualTo(expected.getSecondaryIndexes());
    assertThat(props.saiEnabled()).isEqualTo(expected.getSai());
    assertThat(props.loggedBatchesEnabled()).isEqualTo(expected.getLoggedBatches());
  }

  /**
   * Helper method for doing a Mocked call to fetch DataStoreProperties (including possible retries)
   * and returning the response (if one of calls succeeds), or throwing an exception if all calls
   * fail.
   *
   * @param callsToSucceed Number of the one (1-based) that should succeed; other calls will fail
   * @param callCounter Counter in which number of calls made (succeed and fail) is returned
   * @param response Response to return in case of successful call
   * @return Actual {@code DataStoreProperties} Object call returns
   */
  private DataStoreProperties fetchDataStoreProperties(
      int callsToSucceed, AtomicInteger callCounter, Schema.SupportedFeaturesResponse response) {
    DataStoreConfig config = mock(DataStoreConfig.class);
    when(config.ignoreBridge()).thenReturn(false);

    // Fail first N-1 calls, succeed Nth call (and fail afterwards)
    doAnswer(
            invocation -> {
              int callNr = callCounter.incrementAndGet();
              final StreamObserver<Schema.SupportedFeaturesResponse> observer =
                  invocation.getArgument(1);
              if (callNr == callsToSucceed) {
                observer.onNext(response);
                observer.onCompleted();
              } else {
                observer.onError(new StatusRuntimeException(Status.UNAVAILABLE));
              }
              return null;
            })
        .when(bridgeService)
        .getSupportedFeatures(any(), any());

    return dataStorePropertiesConfiguration.configuration(bridge, config);
  }
}
