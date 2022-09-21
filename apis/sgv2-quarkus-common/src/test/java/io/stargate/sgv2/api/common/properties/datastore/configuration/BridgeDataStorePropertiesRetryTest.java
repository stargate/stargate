package io.stargate.sgv2.api.common.properties.datastore.configuration;

import static org.assertj.core.api.Assertions.assertThat;
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
import io.stargate.sgv2.api.common.BridgeTest;
import io.stargate.sgv2.api.common.config.DataStoreConfig;
import io.stargate.sgv2.api.common.properties.datastore.DataStoreProperties;
import java.util.concurrent.atomic.AtomicInteger;
import javax.inject.Inject;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

@QuarkusTest
public class BridgeDataStorePropertiesRetryTest extends BridgeTest {
  @GrpcClient("bridge")
  StargateBridgeGrpc.StargateBridgeBlockingStub bridge;

  @Inject DataStorePropertiesConfiguration dataStorePropertiesConfiguration;

  protected AtomicInteger mockGetSupportedFeaturesCall(final int succeedOn) {
    Schema.SupportedFeaturesResponse response =
        Schema.SupportedFeaturesResponse.newBuilder()
            .setSecondaryIndexes(false)
            .setSai(false)
            .setLoggedBatches(true)
            .build();

    final AtomicInteger callCounter = new AtomicInteger(0);
    // Fail first call, succeed Nth call (and fail afterwards)
    doAnswer(
            new Answer() {
              private int count = 0;

              public Object answer(InvocationOnMock invocation) {
                int callNr = callCounter.incrementAndGet();
                final StreamObserver<Schema.SupportedFeaturesResponse> observer =
                    invocation.getArgument(1);
                if (callNr == succeedOn) {
                  observer.onNext(response);
                  observer.onCompleted();
                } else {
                  observer.onError(new StatusRuntimeException(Status.UNAVAILABLE));
                }
                return null;
              }
            })
        .when(bridgeService)
        .getSupportedFeatures(any(), any());
    return callCounter;
  }

  @Test
  public void dataStoreWithNoRetriesOk() {
    dataStoreWithNCalls(1);
  }

  @Test
  public void dataStoreWithOneRetryOk() {
    dataStoreWithNCalls(2);
  }

  @Test
  public void dataStoreWithTwoRetriesOk() {
    dataStoreWithNCalls(3);
  }

  private void dataStoreWithNCalls(int callsToSucceed) {
    DataStoreConfig config = mock(DataStoreConfig.class);
    when(config.ignoreBridge()).thenReturn(false);
    final AtomicInteger callCounter = mockGetSupportedFeaturesCall(callsToSucceed);

    DataStoreProperties props = dataStorePropertiesConfiguration.configuration(bridge, config);
    assertThat(props.secondaryIndexesEnabled()).isFalse();
    assertThat(props.saiEnabled()).isFalse();
    assertThat(props.loggedBatchesEnabled()).isTrue();

    assertThat(callCounter.get()).isEqualTo(callsToSucceed);
  }
}
