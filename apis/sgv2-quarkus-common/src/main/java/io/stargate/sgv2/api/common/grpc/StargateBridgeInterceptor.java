package io.stargate.sgv2.api.common.grpc;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.Deadline;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.quarkus.arc.Arc;
import io.quarkus.arc.InjectableBean;
import io.quarkus.arc.InjectableContext;
import io.quarkus.grpc.GlobalInterceptor;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.api.common.config.GrpcConfig;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * ClientInterceptor that enriches request Metadata with tenant ID and cassandra token from
 * StargateRequestInfo.
 */
@GlobalInterceptor
@ApplicationScoped
public class StargateBridgeInterceptor implements ClientInterceptor {

  /** Our {@link GrpcConfig}. */
  @Inject GrpcConfig grpcConfig;

  /** Metadata resolver. */
  @Inject GrpcMetadataResolver metadataResolver;

  /** Request scoped {@link StargateRequestInfo} instance. */
  @Inject StargateRequestInfo requestInfo;

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {

    // get default metadata
    Metadata metadata = metadataResolver.getDefaultMetadata();

    // StargateRequestInfo is request scoped bean
    // there is no better way to know if it's available
    // as we can intercept outside request
    // thus ensure context is active and has instances
    InjectableContext.ContextState contextState =
        Arc.container().requestContext().getStateIfActive();
    boolean contextEmpty =
        null == contextState || !hasStargateRequestBean(contextState.getContextualInstances());

    // if context includes request info, reload metadata
    if (!contextEmpty) {
      metadata = metadataResolver.getMetadata(requestInfo);
    }

    // handle deadlines
    CallOptions callOptionsFinal = callOptionsWithDeadline(callOptions);

    // call with extra metadata and final options
    return new HeaderAttachingClientCall<>(next.newCall(method, callOptionsFinal), metadata);
  }

  // deals with the call timeout if one is defined
  private CallOptions callOptionsWithDeadline(CallOptions callOptions) {
    if (grpcConfig.callDeadline().isPresent()) {
      Duration deadline = grpcConfig.callDeadline().get();
      return callOptions.withDeadline(Deadline.after(deadline.toMillis(), TimeUnit.MILLISECONDS));
    } else {
      return callOptions;
    }
  }

  // ensures contextual instance list contains StargateRequestInfo
  private boolean hasStargateRequestBean(Map<InjectableBean<?>, Object> contextualInstances) {
    for (InjectableBean<?> bean : contextualInstances.keySet()) {
      if (Objects.equals(bean.getBeanClass(), StargateRequestInfo.class)) {
        return true;
      }
    }
    return false;
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
