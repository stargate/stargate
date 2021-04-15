package io.stargate.grpc.service.interceptors;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.UnauthorizedException;
import io.stargate.grpc.service.Service;

public class AuthenticationInterceptor implements ServerInterceptor {
  public static final Metadata.Key<String> TOKEN_KEY =
      Metadata.Key.of("X-Cassandra-Token", Metadata.ASCII_STRING_MARSHALLER);

  private final AuthenticationService authentication;

  public AuthenticationInterceptor(AuthenticationService authentication) {
    this.authentication = authentication;
  }

  @Override
  public <ReqT, RespT> Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    try {
      String token = headers.get(TOKEN_KEY);
      if (token == null) {
        call.close(Status.UNAUTHENTICATED.withDescription("No token provided"), new Metadata());
        return null;
      }
      Context context = Context.current();
      context = context.withValue(Service.AUTHENTICATION_KEY, authentication.validateToken(token));
      return Contexts.interceptCall(context, call, headers, next);
    } catch (UnauthorizedException e) {
      call.close(
          Status.UNAUTHENTICATED.withDescription("Invalid token").withCause(e), new Metadata());
    }
    return null;
  }
}
