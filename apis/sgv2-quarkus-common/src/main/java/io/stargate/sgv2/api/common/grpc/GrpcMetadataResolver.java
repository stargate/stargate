package io.stargate.sgv2.api.common.grpc;

import io.grpc.Metadata;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.api.common.config.GrpcMetadataConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

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

  /** Default metadata. */
  private final Metadata defaultMetadata;

  // TODO add validation @Pattern(regexp = "rest|graphql")  after
  //  https://github.com/quarkusio/quarkus/issues/28783
  @Inject
  public GrpcMetadataResolver(GrpcMetadataConfig config, @SourceApiQualifier String sourceApi) {
    this.tenantIdKey = Metadata.Key.of(config.tenantIdKey(), Metadata.ASCII_STRING_MARSHALLER);
    this.cassandraTokenKey =
        Metadata.Key.of(config.cassandraTokenKey(), Metadata.ASCII_STRING_MARSHALLER);

    // default metadata includes source api
    Metadata.Key<String> sourceApiKey =
        Metadata.Key.of(config.sourceApiKey(), Metadata.ASCII_STRING_MARSHALLER);
    Metadata defaultMetadata = new Metadata();
    defaultMetadata.put(sourceApiKey, sourceApi);
    this.defaultMetadata = defaultMetadata;
  }

  /**
   * @return Returns default metadata, without the information from the {@link StargateRequestInfo}.
   */
  public Metadata getDefaultMetadata() {
    return defaultMetadata;
  }

  /**
   * Returns GRPC metadata for the given {@link StargateRequestInfo}, including the {@link
   * #defaultMetadata}.
   *
   * @param requestInfo Request info.
   * @return Metadata
   */
  public Metadata getMetadata(StargateRequestInfo requestInfo) {
    Metadata metadata = new Metadata();
    requestInfo.getTenantId().ifPresent(t -> metadata.put(tenantIdKey, t));
    requestInfo.getCassandraToken().ifPresent(t -> metadata.put(cassandraTokenKey, t));
    metadata.merge(defaultMetadata);
    return metadata;
  }
}
