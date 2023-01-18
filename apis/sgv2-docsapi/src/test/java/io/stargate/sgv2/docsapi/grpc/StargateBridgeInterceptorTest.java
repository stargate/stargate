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
package io.stargate.sgv2.docsapi.grpc;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableMap;
import io.grpc.Metadata;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.bridge.proto.MutinyStargateBridgeGrpc;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.api.common.testprofiles.FixedTenantTestProfile;
import io.stargate.sgv2.common.bridge.BridgeTest;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.docsapi.api.v2.namespaces.collections.documents.DocumentReadResource;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@QuarkusTest
@TestProfile(StargateBridgeInterceptorTest.Profile.class)
class StargateBridgeInterceptorTest extends BridgeTest {

  public static class Profile extends FixedTenantTestProfile
      implements NoGlobalResourcesTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(super.getConfigOverrides())
          .put("stargate.grpc-metadata.tenant-id-key", "custom-tenant-key")
          .put("stargate.grpc-metadata.cassandra-token-key", "custom-token-key")
          .put("stargate.grpc-metadata.source-api-key", "custom-source-api-key")
          .build();
    }
  }

  @GrpcClient("bridge")
  MutinyStargateBridgeGrpc.MutinyStargateBridgeStub client;

  ArgumentCaptor<Metadata> headersCaptor;

  @BeforeEach
  public void init() {
    headersCaptor = ArgumentCaptor.forClass(Metadata.class);
  }

  @Test
  public void happyPath() {
    // call bridge in during the http call
    String token = RandomStringUtils.randomAlphanumeric(16);
    String keyspaceName = RandomStringUtils.randomAlphanumeric(16);
    Schema.CqlKeyspaceDescribe response =
        Schema.CqlKeyspaceDescribe.newBuilder()
            .setCqlKeyspace(Schema.CqlKeyspace.newBuilder().setName(keyspaceName).build())
            .buildPartial();
    doAnswer(
            invocationOnMock -> {
              StreamObserver<Schema.CqlKeyspaceDescribe> observer = invocationOnMock.getArgument(1);
              observer.onNext(response);
              observer.onCompleted();
              return null;
            })
        .when(bridgeService)
        .describeKeyspace(any(), any());

    // fake document read, to fetch needed keyspace
    given()
        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, token)
        .queryParam("raw", true)
        .when()
        .get(DocumentReadResource.BASE_PATH + "/{collection}/id", keyspaceName, "collection");

    // verify metadata
    verify(bridgeInterceptor).interceptCall(any(), headersCaptor.capture(), any());
    assertThat(headersCaptor.getAllValues())
        .singleElement()
        .satisfies(
            metadata -> {
              Metadata.Key<String> tenantKey =
                  Metadata.Key.of("custom-tenant-key", Metadata.ASCII_STRING_MARSHALLER);
              assertThat(metadata.get(tenantKey)).isEqualTo(FixedTenantTestProfile.TENANT_ID);
              Metadata.Key<String> tokenKey =
                  Metadata.Key.of("custom-token-key", Metadata.ASCII_STRING_MARSHALLER);
              assertThat(metadata.get(tokenKey)).isEqualTo(token);
              Metadata.Key<String> sourceApiKey =
                  Metadata.Key.of("custom-source-api-key", Metadata.ASCII_STRING_MARSHALLER);
              assertThat(metadata.get(sourceApiKey)).isEqualTo("rest");
            });
  }

  @Test
  public void noExtraMetadata() {
    // call bridge without http call
    String keyspaceName = RandomStringUtils.randomAlphanumeric(16);
    Schema.DescribeKeyspaceQuery query =
        Schema.DescribeKeyspaceQuery.newBuilder().setKeyspaceName(keyspaceName).build();
    Schema.CqlKeyspaceDescribe response =
        Schema.CqlKeyspaceDescribe.newBuilder()
            .setCqlKeyspace(Schema.CqlKeyspace.newBuilder().setName(keyspaceName).build())
            .buildPartial();
    doAnswer(
            invocationOnMock -> {
              StreamObserver<Schema.CqlKeyspaceDescribe> observer = invocationOnMock.getArgument(1);
              observer.onNext(response);
              observer.onCompleted();
              return null;
            })
        .when(bridgeService)
        .describeKeyspace(any(), any());

    UniAssertSubscriber<Schema.CqlKeyspaceDescribe> result =
        client.describeKeyspace(query).subscribe().withSubscriber(UniAssertSubscriber.create());

    // verify result
    result.awaitItem().assertItem(response).assertCompleted();

    // verify metadata
    verify(bridgeInterceptor).interceptCall(any(), headersCaptor.capture(), any());
    assertThat(headersCaptor.getAllValues())
        .singleElement()
        .satisfies(
            metadata -> {
              Metadata.Key<String> tenantKey =
                  Metadata.Key.of("custom-tenant-key", Metadata.ASCII_STRING_MARSHALLER);
              assertThat(metadata.containsKey(tenantKey)).isFalse();
              Metadata.Key<String> tokenKey =
                  Metadata.Key.of("custom-token-key", Metadata.ASCII_STRING_MARSHALLER);
              assertThat(metadata.containsKey(tokenKey)).isFalse();
              Metadata.Key<String> sourceApiKey =
                  Metadata.Key.of("custom-source-api-key", Metadata.ASCII_STRING_MARSHALLER);
              assertThat(metadata.get(sourceApiKey)).isEqualTo("rest");
            });
  }
}
