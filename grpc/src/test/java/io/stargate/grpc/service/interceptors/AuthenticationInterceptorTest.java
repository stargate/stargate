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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.AuthenticatedUser;
import io.stargate.grpc.service.Service;
import org.junit.jupiter.api.Test;

public class AuthenticationInterceptorTest {
  @Test
  public void correctCredentials() throws UnauthorizedException {
    AuthenticatedUser authenticatedUser = mock(AuthenticatedUser.class);
    when(authenticatedUser.name()).thenReturn("def");

    AuthenticationSubject authenticationSubject = mock(AuthenticationSubject.class);
    when(authenticationSubject.asUser()).thenReturn(authenticatedUser);

    AuthenticationService authenticationService = mock(AuthenticationService.class);
    when(authenticationService.validateToken("abc")).thenReturn(authenticationSubject);

    ServerCall call = mock(ServerCall.class);

    Metadata metadata = new Metadata();
    metadata.put(AuthenticationInterceptor.TOKEN_KEY, "abc");

    AuthenticationInterceptor interceptor = new AuthenticationInterceptor(authenticationService);
    interceptor.interceptCall(
        call,
        metadata,
        (c, h) -> {
          AuthenticationSubject subject = Service.AUTHENTICATION_KEY.get();
          assertThat(subject).isNotNull();
          AuthenticatedUser user = subject.asUser();
          assertThat(user.name()).isEqualTo("def");
          return null;
        });

    verify(call, never()).close(any(Status.class), any(Metadata.class));
    verify(authenticatedUser, times(1)).name();
  }

  @Test
  public void emptyCredentials() throws UnauthorizedException {
    AuthenticationService authenticationService = mock(AuthenticationService.class);

    ServerCallHandler next = mock(ServerCallHandler.class);
    ServerCall call = mock(ServerCall.class);

    Metadata metadata = new Metadata();
    AuthenticationInterceptor interceptor = new AuthenticationInterceptor(authenticationService);
    interceptor.interceptCall(call, metadata, next);

    verify(call, times(1))
        .close(
            argThat(
                s ->
                    s.getCode() == Status.UNAUTHENTICATED.getCode()
                        && s.getDescription().equals("No token provided")),
            any(Metadata.class));
    verify(authenticationService, never()).validateToken(any(String.class));
    verify(next, never()).startCall(any(ServerCall.class), any(Metadata.class));
  }

  @Test
  public void invalidCredentials() throws UnauthorizedException {
    AuthenticationService authenticationService = mock(AuthenticationService.class);
    when(authenticationService.validateToken("invalid")).thenThrow(new UnauthorizedException(""));

    ServerCallHandler next = mock(ServerCallHandler.class);
    ServerCall call = mock(ServerCall.class);

    Metadata metadata = new Metadata();
    metadata.put(AuthenticationInterceptor.TOKEN_KEY, "invalid");
    AuthenticationInterceptor interceptor = new AuthenticationInterceptor(authenticationService);
    interceptor.interceptCall(call, metadata, next);

    verify(call, times(1))
        .close(
            argThat(
                s ->
                    s.getCode() == Status.UNAUTHENTICATED.getCode()
                        && s.getDescription().equals("Invalid token")),
            any(Metadata.class));
    verify(authenticationService, times(1)).validateToken("invalid");
    verify(next, never()).startCall(any(ServerCall.class), any(Metadata.class));
  }
}
