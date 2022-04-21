package io.stargate.sgv2.docsapi.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.grpc.Metadata;
import io.grpc.stub.StreamObserver;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.mockito.InjectMock;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.proto.Schema;
import io.stargate.proto.StargateBridge;
import io.stargate.sgv2.docsapi.BridgeTest;
import io.stargate.sgv2.docsapi.api.common.StargateRequestInfo;
import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@QuarkusTest
@TestProfile(GrpcClientsTest.Profile.class)
class GrpcClientsTest extends BridgeTest {

  public static class Profile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .put("stargate.grpc-metadata.tenant-id-key", "custom-tenant-key")
          .put("stargate.grpc-metadata.cassandra-token-key", "custom-token-key")
          .build();
    }
  }

  @Inject GrpcClients grpcClients;

  @InjectMock StargateRequestInfo requestInfo;

  ArgumentCaptor<Metadata> headersCaptor;

  @BeforeEach
  public void init() {
    headersCaptor = ArgumentCaptor.forClass(Metadata.class);
  }

  @Test
  public void happyPath() {
    String token = RandomStringUtils.randomAlphanumeric(16);
    String tenant = RandomStringUtils.randomAlphanumeric(16);
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
    when(requestInfo.getCassandraToken()).thenReturn(Optional.of(token));
    when(requestInfo.getTenantId()).thenReturn(Optional.of(tenant));

    StargateBridge client = grpcClients.bridgeClient(requestInfo);
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
              assertThat(metadata.get(tenantKey)).isEqualTo(tenant);
              Metadata.Key<String> tokenKey =
                  Metadata.Key.of("custom-token-key", Metadata.ASCII_STRING_MARSHALLER);
              assertThat(metadata.get(tokenKey)).isEqualTo(token);
            });
  }

  @Test
  public void noExtraMetadat() {
    String token = RandomStringUtils.randomAlphanumeric(16);
    String tenant = RandomStringUtils.randomAlphanumeric(16);
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

    StargateBridge client = grpcClients.bridgeClient(requestInfo);
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
            });
  }
}
