package io.stargate.sgv2.docsapi.api.common.properties.datastore.configuration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import io.stargate.proto.Schema;
import io.stargate.proto.StargateBridgeGrpc;
import io.stargate.sgv2.docsapi.BridgeTest;
import io.stargate.sgv2.docsapi.api.common.properties.datastore.DataStoreProperties;
import io.stargate.sgv2.docsapi.config.DataStoreConfig;
import javax.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
class BridgeDataStorePropertiesConfigurationTest extends BridgeTest {

  @GrpcClient("bridge")
  StargateBridgeGrpc.StargateBridgeBlockingStub bridge;

  @Inject DataStorePropertiesConfiguration dataStorePropertiesConfiguration;

  @BeforeEach
  public void mockBridge() {
    Schema.SupportedFeaturesResponse response =
        Schema.SupportedFeaturesResponse.newBuilder()
            .setSecondaryIndexes(false)
            .setSai(true)
            .setLoggedBatches(false)
            .build();

    doAnswer(
            invocationOnMock -> {
              StreamObserver<Schema.SupportedFeaturesResponse> observer =
                  invocationOnMock.getArgument(1);
              observer.onNext(response);
              observer.onCompleted();
              return null;
            })
        .when(bridgeService)
        .getSupportedFeatures(any(), any());
  }

  @Test
  public void dataStoreFromBridge() {
    DataStoreConfig config = mock(DataStoreConfig.class);
    when(config.ignoreBridge()).thenReturn(false);

    DataStoreProperties dataStoreProperties =
        dataStorePropertiesConfiguration.configuration(bridge, config);

    assertThat(dataStoreProperties.secondaryIndexesEnabled()).isFalse();
    assertThat(dataStoreProperties.saiEnabled()).isTrue();
    assertThat(dataStoreProperties.loggedBatchesEnabled()).isFalse();
  }
}
