package io.stargate.grpc.service.interceptors;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.stargate.grpc.service.Service;

public class RemoteAddressInterceptor implements ServerInterceptor {
  @Override
  public <ReqT, RespT> Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    Context context = Context.current();
    context =
        context.withValue(
            Service.REMOTE_ADDRESS_KEY, call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR));
    return Contexts.interceptCall(context, call, headers, next);
  }
}
