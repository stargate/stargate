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
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.stargate.sgv2.api.common.grpc;

import static io.stargate.bridge.proto.QueryOuterClass.Query;
import static io.stargate.bridge.proto.QueryOuterClass.Response;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectMock;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.bridge.proto.StargateBridge;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.api.common.schema.SchemaManager;
import jakarta.inject.Inject;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class StargateBridgeClientTest {

  @InjectMock SchemaManager schemaManager;

  StargateBridge reactiveBridge;

  @InjectMock StargateRequestInfo requestInfo;

  @Inject StargateBridgeClient client;

  @BeforeEach
  public void setup() {
    reactiveBridge = mock(StargateBridge.class);
    when(requestInfo.getStargateBridge()).thenReturn(reactiveBridge);
  }

  @Test
  public void shouldForwardCqlQuery() {
    // Given
    Query query = Query.getDefaultInstance();
    Response expectedResponse = Response.getDefaultInstance();
    when(reactiveBridge.executeQuery(query)).thenReturn(Uni.createFrom().item(expectedResponse));

    // When
    Response actualResponse = client.executeQuery(query);

    // Then
    assertThat(actualResponse).isSameAs(expectedResponse);
  }

  @Test
  public void shouldForwardSchemaLookup() {
    // Given
    String keyspaceName = "ks";
    CqlKeyspaceDescribe expectedKeyspace = CqlKeyspaceDescribe.getDefaultInstance();
    when(schemaManager.getKeyspace(keyspaceName))
        .thenReturn(Uni.createFrom().item(expectedKeyspace));

    // When
    Optional<CqlKeyspaceDescribe> actualKeyspace = client.getKeyspace(keyspaceName, false);

    // Then
    assertThat(actualKeyspace).hasValueSatisfying(ks -> assertThat(ks).isSameAs(expectedKeyspace));
  }
}
