package io.stargate.sgv2.api.common.grpc;

import io.grpc.Metadata;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.api.common.config.GrpcMetadataConfig;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/**
 * Component responsible for resolving needed Metadata to be passed to the Bridge, based on the
 * {@link StargateRequestInfo}.
 */
@ApplicationScoped
public class GrpcMetadataResolver {

  /** Metadata key for passing the tenant-id to the Bridge. */
  private final Metadata.Key<String> tenantIdKey;

  /** Metadata key for passing the cassandra token to the Bridge. */
  private final Metadata.Key<String> cassandraTokenKey;

  @Inject
  public GrpcMetadataResolver(GrpcMetadataConfig config) {
    this.tenantIdKey = Metadata.Key.of(config.tenantIdKey(), Metadata.ASCII_STRING_MARSHALLER);
    this.cassandraTokenKey =
        Metadata.Key.of(config.cassandraTokenKey(), Metadata.ASCII_STRING_MARSHALLER);
  }

  /**
   * Returns GRPC metadata for the given {@link StargateRequestInfo}.
   *
   * @param requestInfo Request info.
   * @return Metadata
   */
  public Metadata getMetadata(StargateRequestInfo requestInfo) {
    Metadata metadata = new Metadata();
    requestInfo.getTenantId().ifPresent(t -> metadata.put(tenantIdKey, t));
    requestInfo.getCassandraToken().ifPresent(t -> metadata.put(cassandraTokenKey, t));
    return metadata;
  }
}
