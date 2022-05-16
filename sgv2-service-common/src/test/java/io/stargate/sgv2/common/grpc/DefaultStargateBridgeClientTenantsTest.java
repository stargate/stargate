package io.stargate.sgv2.common.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.protobuf.Int32Value;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.stargate.bridge.proto.Schema;
import io.stargate.bridge.proto.Schema.CqlKeyspace;
import io.stargate.bridge.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.bridge.proto.Schema.DescribeKeyspaceQuery;
import io.stargate.bridge.proto.StargateBridgeGrpc;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.codec.binary.Hex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Covers the client's keyspace cache in multi-tenant scenarios.
 *
 * <p>This is treated separately because tenant ids are passed as channel metadata, which can't
 * easily be mocked with Mockito. Instead, we reimplement an actual gRPC service.
 */
public class DefaultStargateBridgeClientTenantsTest {

  private static final String TENANT1 = "tenant1";
  private static final String TENANT2 = "tenant2";
  private static final String TENANT1_PREFIX =
      Hex.encodeHexString(TENANT1.getBytes(StandardCharsets.UTF_8));
  private static final String TENANT2_PREFIX =
      Hex.encodeHexString(TENANT2.getBytes(StandardCharsets.UTF_8));

  private static final String SERVER_NAME = "MockBridge";
  private static final String AUTH_TOKEN = "MockAuthToken";
  private static final Schema.SchemaRead.SourceApi SOURCE_API = Schema.SchemaRead.SourceApi.REST;

  private Server server;
  private ManagedChannel channel;
  private MockBridgeService service;
  private Cache<String, CqlKeyspaceDescribe> keyspaceCache;

  @BeforeEach
  public void setup() throws IOException {
    service = spy(new MockBridgeService());
    server =
        InProcessServerBuilder.forName(SERVER_NAME)
            .directExecutor()
            .intercept(new TenantInterceptor())
            .addService(service)
            .build();
    server.start();
    channel = InProcessChannelBuilder.forName(SERVER_NAME).usePlaintext().build();
    keyspaceCache = Caffeine.newBuilder().build();
  }

  @AfterEach
  public void teardown() {
    server.shutdownNow();
    channel.shutdownNow();
  }

  @Test
  public void getKeyspace() {
    // Given
    String keyspaceName = "ks";
    service.mockKeyspace(TENANT1, keyspaceName);
    StargateBridgeClient client = newClient(TENANT1);

    // When
    Optional<CqlKeyspaceDescribe> maybeDescription = client.getKeyspace(keyspaceName, false);

    // Then
    assertThat(maybeDescription)
        .hasValueSatisfying(
            description -> {
              // Keyspace contains the expected info:
              CqlKeyspace keyspace = description.getCqlKeyspace();
              assertThat(keyspace.getName()).isEqualTo(keyspaceName);
              assertThat(keyspace.getGlobalName()).isEqualTo(TENANT1_PREFIX + "_ks");

              // And it was cached:
              assertThat(keyspaceCache.getIfPresent(keyspace.getGlobalName()))
                  .isEqualTo(description);
            });
    verify(service)
        .describeKeyspace(
            eq(DescribeKeyspaceQuery.newBuilder().setKeyspaceName(keyspaceName).build()), any());
  }

  @Test
  public void getKeyspaceAlreadyCached() {
    // Given
    String keyspaceName = "ks";
    service.mockKeyspace(TENANT1, keyspaceName);
    StargateBridgeClient client = newClient(TENANT1);

    // When
    client.getKeyspace(keyspaceName, false);
    client.getKeyspace(keyspaceName, false);

    // Then
    verify(service, times(1))
        .describeKeyspace(
            eq(DescribeKeyspaceQuery.newBuilder().setKeyspaceName(keyspaceName).build()), any());
  }

  @Test
  public void getKeyspaceDifferentTenants() {
    // Given
    String keyspaceName = "ks";
    service.mockKeyspace(TENANT1, keyspaceName);
    service.mockKeyspace(TENANT2, keyspaceName);
    StargateBridgeClient client1 = newClient(TENANT1);
    StargateBridgeClient client2 = newClient(TENANT2);

    // When
    Optional<CqlKeyspaceDescribe> maybeDescription1 = client1.getKeyspace(keyspaceName, false);
    Optional<CqlKeyspaceDescribe> maybeDescription2 = client2.getKeyspace(keyspaceName, false);

    // Then
    assertThat(maybeDescription1)
        .hasValueSatisfying(
            description -> {
              CqlKeyspace keyspace = description.getCqlKeyspace();
              assertThat(keyspace.getName()).isEqualTo(keyspaceName);
              assertThat(keyspace.getGlobalName()).isEqualTo(TENANT1_PREFIX + "_ks");

              assertThat(keyspaceCache.getIfPresent(keyspace.getGlobalName()))
                  .isEqualTo(description);
            });
    assertThat(maybeDescription2)
        .hasValueSatisfying(
            description -> {
              CqlKeyspace keyspace = description.getCqlKeyspace();
              assertThat(keyspace.getName()).isEqualTo(keyspaceName);
              assertThat(keyspace.getGlobalName()).isEqualTo(TENANT2_PREFIX + "_ks");

              assertThat(keyspaceCache.getIfPresent(keyspace.getGlobalName()))
                  .isEqualTo(description);
            });
    verify(service, times(2))
        .describeKeyspace(
            eq(DescribeKeyspaceQuery.newBuilder().setKeyspaceName(keyspaceName).build()), any());
  }

  private StargateBridgeClient newClient(String tenantId) {
    return new DefaultStargateBridgeClient(
        channel,
        AUTH_TOKEN,
        Optional.of(tenantId),
        keyspaceCache,
        new LazyReference<>(),
        SOURCE_API);
  }

  static class MockBridgeService extends StargateBridgeGrpc.StargateBridgeImplBase {

    private final Map<KeyspaceCoordinates, CqlKeyspaceDescribe> keyspaces =
        new ConcurrentHashMap<>();

    void mockKeyspace(String tenantId, String keyspaceName) {
      keyspaces.put(
          new KeyspaceCoordinates(tenantId, keyspaceName),
          CqlKeyspaceDescribe.newBuilder()
              .setCqlKeyspace(
                  CqlKeyspace.newBuilder()
                      .setName(keyspaceName)
                      .setGlobalName(encode(tenantId) + "_" + keyspaceName))
              .setHash(Int32Value.of(keyspaceName.hashCode()))
              .build());
    }

    @Override
    public void describeKeyspace(
        DescribeKeyspaceQuery request, StreamObserver<CqlKeyspaceDescribe> responseObserver) {
      String tenantId = TenantInterceptor.TENANT_KEY.get();
      String keyspaceName = request.getKeyspaceName();
      CqlKeyspaceDescribe keyspace = keyspaces.get(new KeyspaceCoordinates(tenantId, keyspaceName));
      if (keyspace == null) {
        responseObserver.onError(
            Status.NOT_FOUND.withDescription("Keyspace not found").asException());
      } else {
        responseObserver.onNext(keyspace);
        responseObserver.onCompleted();
      }
    }
  }

  static class KeyspaceCoordinates {
    final String tenantId;
    final String keyspaceName;

    KeyspaceCoordinates(String tenantId, String keyspaceName) {
      this.tenantId = tenantId;
      this.keyspaceName = keyspaceName;
    }

    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      } else if (other instanceof KeyspaceCoordinates) {
        KeyspaceCoordinates that = (KeyspaceCoordinates) other;
        return Objects.equals(this.tenantId, that.tenantId)
            && Objects.equals(this.keyspaceName, that.keyspaceName);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(tenantId, keyspaceName);
    }
  }

  static class TenantInterceptor implements ServerInterceptor {
    static final Context.Key<String> TENANT_KEY = Context.key("tenantId");

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
        ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
      String tenantId = headers.get(DefaultStargateBridgeClient.TENANT_ID_KEY);
      return Contexts.interceptCall(
          Context.current().withValue(TENANT_KEY, tenantId), call, headers, next);
    }
  }

  private static String encode(String tenantName) {
    return Hex.encodeHexString(tenantName.getBytes(StandardCharsets.UTF_8));
  }
}
