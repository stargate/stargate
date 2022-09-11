/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.bridge.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.when;

import io.stargate.bridge.proto.Schema.SupportedFeaturesRequest;
import io.stargate.bridge.proto.Schema.SupportedFeaturesResponse;
import io.stargate.bridge.proto.StargateBridgeGrpc;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class SupportedFeaturesTest extends BaseBridgeTest {

  @ParameterizedTest
  @MethodSource("getSupportedFeatures")
  public void shouldGetSupportedFeatures(
      boolean secondaryIndexes, boolean sai, boolean loggedBatches) {
    // Given
    StargateBridgeGrpc.StargateBridgeBlockingStub stub = makeBlockingStub();
    when(persistence.supportsSecondaryIndex()).thenReturn(secondaryIndexes);
    when(persistence.supportsSAI()).thenReturn(sai);
    when(persistence.supportsLoggedBatches()).thenReturn(loggedBatches);

    startServer(persistence);

    // When
    SupportedFeaturesResponse response =
        stub.getSupportedFeatures(SupportedFeaturesRequest.newBuilder().build());

    // Then
    assertThat(response.getSecondaryIndexes()).isEqualTo(secondaryIndexes);
    assertThat(response.getSai()).isEqualTo(sai);
    assertThat(response.getLoggedBatches()).isEqualTo(loggedBatches);
  }

  public static Arguments[] getSupportedFeatures() {
    return new Arguments[] {
      arguments(false, false, false),
      arguments(false, false, true),
      arguments(false, true, false),
      arguments(false, true, true),
      arguments(true, false, false),
      arguments(true, false, true),
      arguments(true, true, false),
      arguments(true, true, true),
    };
  }
}
