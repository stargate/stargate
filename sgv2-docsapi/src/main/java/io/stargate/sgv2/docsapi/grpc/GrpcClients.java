package io.stargate.sgv2.docsapi.grpc;

import io.grpc.Metadata;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.grpc.GrpcClientUtils;
import io.stargate.bridge.proto.StargateBridge;
import io.stargate.sgv2.docsapi.config.GrpcMetadataConfig;
import java.util.Optional;
import javax.enterprise.context.ApplicationScoped;

/**
 * Bean that holds all the gRPC clients needed, with possibility to enrich the metadata based on the
 * request context.
 */
@ApplicationScoped
public class GrpcClients {

  /** Bridge client. Uses the <code>quarkus.grpc.clients.bridge</code> properties. */
  private final StargateBridge bridge;

  /** Metadata key for passing the tenant-id to the Bridge. */
  private final Metadata.Key<String> tenantIdKey;

  /** Metadata key for passing the cassandra token to the Bridge. */
  private final Metadata.Key<String> cassandraTokenKey;

  public GrpcClients(@GrpcClient("bridge") StargateBridge bridge, GrpcMetadataConfig config) {
    this.tenantIdKey = Metadata.Key.of(config.tenantIdKey(), Metadata.ASCII_STRING_MARSHALLER);
    this.cassandraTokenKey =
        Metadata.Key.of(config.cassandraTokenKey(), Metadata.ASCII_STRING_MARSHALLER);
    this.bridge = bridge;
  }

  /**
   * Returns the reactive gRPC Bridge Client, with attached information about the tenant and C*
   * token.
   *
   * @param tenantId tenant id
   * @param cassandraToken cassandra token
   * @return StargateBridge Reactive Bridge stub
   */
  public StargateBridge bridgeClient(Optional<String> tenantId, Optional<String> cassandraToken) {
    if (tenantId.isEmpty() && cassandraToken.isEmpty()) {
      return bridge;
    }

    Metadata metadata = new Metadata();
    tenantId.ifPresent(t -> metadata.put(tenantIdKey, t));
    cassandraToken.ifPresent(t -> metadata.put(cassandraTokenKey, t));
    return GrpcClientUtils.attachHeaders(bridge, metadata);
  }
}
