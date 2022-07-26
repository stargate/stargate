package io.stargate.sgv2.api.common.grpc;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.quarkus.grpc.GlobalInterceptor;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@GlobalInterceptor
@ApplicationScoped
public class StargateBridgeInterceptor implements ClientInterceptor {
  @Inject StargateRequestInfo requestInfo;
  @Inject GrpcClients grpcClients;

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
    Metadata.Key<String> tenantIdKey = grpcClients.getTenantIdKey();
    Metadata.Key<String> cassandraTokenKey = grpcClients.getCassandraTokenKey();
    callOptions.withCallCredentials(
        new StargateRequestCallCredentials(
            tenantIdKey,
            cassandraTokenKey,
            requestInfo.getTenantId(),
            requestInfo.getCassandraToken()));
    return next.newCall(method, callOptions);
  }
}
