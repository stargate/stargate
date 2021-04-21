package io.stargate.grpc.service;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.db.AuthenticatedUser;

public class MockInterceptor implements ServerInterceptor {
  @Override
  public <ReqT, RespT> Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    AuthenticationSubject subject = mock(AuthenticationSubject.class);
    AuthenticatedUser user = mock(AuthenticatedUser.class);
    when(user.name()).thenReturn("user1");
    when(user.token()).thenReturn("token1");
    when(user.isFromExternalAuth()).thenReturn(true);
    when(subject.asUser()).thenReturn(user);
    Context context = Context.current();
    context = context.withValue(Service.AUTHENTICATION_KEY, subject);
    context =
        context.withValue(
            Service.REMOTE_ADDRESS_KEY, call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR));
    return Contexts.interceptCall(context, call, headers, next);
  }
}
