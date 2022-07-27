package io.stargate.sgv2.api.common.grpc;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.quarkus.grpc.GlobalInterceptor;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.api.common.config.GrpcMetadataConfig;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;

/**
 * ClientInterceptor that enriches request Metadata with tenant ID and cassandra token from
 * StargateRequestInfo.
 */
@GlobalInterceptor
@ApplicationScoped
public class StargateBridgeInterceptor implements ClientInterceptor {
  private Metadata extraHeaders;

  /** Metadata key for passing the tenant-id to the Bridge. */
  private Metadata.Key<String> tenantIdKey;

  /** Metadata key for passing the cassandra token to the Bridge. */
  private Metadata.Key<String> cassandraTokenKey;

  private Instance<StargateRequestInfo> requestInfoInstance;

  @Inject
  public StargateBridgeInterceptor(
      Instance<StargateRequestInfo> requestInfoInstance, GrpcMetadataConfig config) {
    this.tenantIdKey = Metadata.Key.of(config.tenantIdKey(), Metadata.ASCII_STRING_MARSHALLER);
    this.cassandraTokenKey =
        Metadata.Key.of(config.cassandraTokenKey(), Metadata.ASCII_STRING_MARSHALLER);
    this.requestInfoInstance = requestInfoInstance;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
    Metadata metadata = new Metadata();
    if (!requestInfoInstance.isUnsatisfied()) {
      requestInfoInstance.get().getTenantId().ifPresent(t -> metadata.put(tenantIdKey, t));
      requestInfoInstance
          .get()
          .getCassandraToken()
          .ifPresent(t -> metadata.put(cassandraTokenKey, t));
    }
    return new HeaderAttachingClientCall(next.newCall(method, callOptions), metadata);
  }

  private final class HeaderAttachingClientCall<ReqT, RespT>
      extends ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT> {
    HeaderAttachingClientCall(ClientCall<ReqT, RespT> call, Metadata extraHeaders) {
      super(call);
      StargateBridgeInterceptor.this.extraHeaders = extraHeaders;
    }

    public void start(ClientCall.Listener<RespT> responseListener, Metadata headers) {
      if (StargateBridgeInterceptor.this.extraHeaders != null) {
        headers.merge(StargateBridgeInterceptor.this.extraHeaders);
      }
      super.start(responseListener, headers);
    }
  }
}
