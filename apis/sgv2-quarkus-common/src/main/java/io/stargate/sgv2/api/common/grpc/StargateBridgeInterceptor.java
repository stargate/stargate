package io.stargate.sgv2.api.common.grpc;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.quarkus.arc.Arc;
import io.quarkus.arc.InjectableContext;
import io.quarkus.grpc.GlobalInterceptor;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.api.common.config.GrpcMetadataConfig;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/**
 * ClientInterceptor that enriches request Metadata with tenant ID and cassandra token from
 * StargateRequestInfo.
 */
@GlobalInterceptor
@ApplicationScoped
public class StargateBridgeInterceptor implements ClientInterceptor {

  /** Metadata key for passing the tenant-id to the Bridge. */
  private final Metadata.Key<String> tenantIdKey;

  /** Metadata key for passing the cassandra token to the Bridge. */
  private final Metadata.Key<String> cassandraTokenKey;

  /** Request scoped {@link StargateRequestInfo} instance. */
  private final StargateRequestInfo requestInfo;

  @Inject
  public StargateBridgeInterceptor(StargateRequestInfo requestInfo, GrpcMetadataConfig config) {
    this.tenantIdKey = Metadata.Key.of(config.tenantIdKey(), Metadata.ASCII_STRING_MARSHALLER);
    this.cassandraTokenKey =
        Metadata.Key.of(config.cassandraTokenKey(), Metadata.ASCII_STRING_MARSHALLER);
    this.requestInfo = requestInfo;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {

    // StargateRequestInfo is request scoped bean
    // there is no better way to know if if's available
    // as we can intercept outside request
    // thus ensure context is active and has instances
    InjectableContext.ContextState contextState =
        Arc.container().requestContext().getStateIfActive();
    if (null == contextState || contextState.getContextualInstances().isEmpty()) {
      return next.newCall(method, callOptions);
    }

    // resolve and construct metadata
    Metadata metadata = new Metadata();
    requestInfo.getTenantId().ifPresent(t -> metadata.put(tenantIdKey, t));
    requestInfo.getCassandraToken().ifPresent(t -> metadata.put(cassandraTokenKey, t));

    // call with extra metadata
    return new HeaderAttachingClientCall<>(next.newCall(method, callOptions), metadata);
  }

  private static final class HeaderAttachingClientCall<ReqT, RespT>
      extends ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT> {

    private final Metadata extraHeaders;

    HeaderAttachingClientCall(ClientCall<ReqT, RespT> call, Metadata extraHeaders) {
      super(call);
      this.extraHeaders = extraHeaders;
    }

    public void start(ClientCall.Listener<RespT> responseListener, Metadata headers) {
      if (this.extraHeaders != null) {
        headers.merge(this.extraHeaders);
      }
      super.start(responseListener, headers);
    }
  }
}
