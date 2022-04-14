package io.stargate.sgv2.docsapi.grpc;

import io.grpc.Metadata;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.grpc.GrpcClientUtils;
import io.stargate.proto.MutinyStargateBridgeGrpc;
import io.stargate.sgv2.docsapi.api.common.StargateRequestInfo;
import io.stargate.sgv2.docsapi.config.GrpcMetadataConfig;
import javax.enterprise.context.ApplicationScoped;

/**
 * Bean that holds all the gRPC clients needed, with possibility to enrich the metadata based on the
 * request context.
 */
@ApplicationScoped
public class GrpcClients {

  /** Bridge client. Uses the <code>quarkus.grpc.clients.bridge</code> properties. */
  private final MutinyStargateBridgeGrpc.MutinyStargateBridgeStub bridge;

  /** Metadata key for passing the tenant-id to the Bridge. */
  private final Metadata.Key<String> tenantIdKey;

  /** Metadata key for passing the cassandra token to the Bridge. */
  private final Metadata.Key<String> cassandraTokenKey;

  public GrpcClients(
      @GrpcClient("bridge") MutinyStargateBridgeGrpc.MutinyStargateBridgeStub bridge,
      GrpcMetadataConfig config) {
    this.tenantIdKey = Metadata.Key.of(config.tenantIdKey(), Metadata.ASCII_STRING_MARSHALLER);
    this.cassandraTokenKey =
        Metadata.Key.of(config.cassandraTokenKey(), Metadata.ASCII_STRING_MARSHALLER);
    this.bridge = bridge;
  }

  /**
   * Returns the reactive gRPC Bridge Client, with attached information from the {@link
   * StargateRequestInfo}.
   *
   * @param requestInfo {@link StargateRequestInfo}
   * @return MutinyStargateBridgeGrpc.MutinyStargateBridgeStub Reactive Bridge stub
   */
  public MutinyStargateBridgeGrpc.MutinyStargateBridgeStub bridgeClient(
      StargateRequestInfo requestInfo) {
    if (requestInfo.getTenantId().isEmpty() && requestInfo.getCassandraToken().isEmpty()) {
      return bridge;
    }

    Metadata metadata = new Metadata();
    requestInfo.getTenantId().ifPresent(t -> metadata.put(tenantIdKey, t));
    requestInfo.getCassandraToken().ifPresent(t -> metadata.put(cassandraTokenKey, t));
    return GrpcClientUtils.attachHeaders(bridge, metadata);
  }
}
