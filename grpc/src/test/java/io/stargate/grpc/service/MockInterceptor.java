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
    when(user.isFromExternalAuth()).thenReturn(true);
    when(subject.asUser()).thenReturn(user);
    Context context = Context.current();
    context = context.withValue(GrpcService.AUTHENTICATION_KEY, subject);
    context =
        context.withValue(
            GrpcService.REMOTE_ADDRESS_KEY,
            call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR));
    return Contexts.interceptCall(context, call, headers, next);
  }
}
