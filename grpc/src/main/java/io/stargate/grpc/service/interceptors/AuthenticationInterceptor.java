/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

/**
 * An interceptor that checks calls for per-call credentials. The metadata header {@code
 * "X-Cassandra-Token"} is used to pass the credentials to the RPC calls. The value is authenticated
 * using {@link AuthenticationService#validateToken(String)}. The resulting {@link
 * io.stargate.auth.AuthenticationSubject} is bound to the call context using the key {@link
 * Service#AUTHENTICATION_KEY}.
 */
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
