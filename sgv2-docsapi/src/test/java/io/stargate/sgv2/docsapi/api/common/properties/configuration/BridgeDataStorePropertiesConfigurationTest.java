package io.stargate.sgv2.docsapi.api.common.properties.configuration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

import com.google.common.collect.ImmutableMap;
import io.grpc.stub.StreamObserver;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.stargate.proto.Schema;
import io.stargate.sgv2.docsapi.BridgeTest;
import io.stargate.sgv2.docsapi.api.common.properties.datastore.DataStoreProperties;
import java.util.Map;
import javax.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(BridgeDataStorePropertiesConfigurationTest.Profile.class)
class BridgeDataStorePropertiesConfigurationTest extends BridgeTest {

  @Inject DataStoreProperties dataStoreProperties;

  public static class Profile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .put("stargate.data-store.ignore-bridge", "false")
          .build();
    }
  }

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
    assertThat(dataStoreProperties.secondaryIndexesEnabled()).isFalse();
    assertThat(dataStoreProperties.saiEnabled()).isTrue();
    assertThat(dataStoreProperties.loggedBatchesEnabled()).isFalse();
  }
}
