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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.AuthenticatedUser;
import io.stargate.db.Parameters;
import io.stargate.db.Result;
import io.stargate.db.Statement;
import io.stargate.grpc.StargateBearerToken;
import io.stargate.grpc.Utils;
import io.stargate.grpc.service.interceptors.NewConnectionInterceptor;
import io.stargate.proto.StargateGrpc;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;

public class HeadersTest extends BaseGrpcServiceTest {

  private static final Key<String> HEADER1_KEY =
      Key.of("header1", Metadata.ASCII_STRING_MARSHALLER);
  private static final Key<String> HEADER2_KEY =
      Key.of("header2", Metadata.ASCII_STRING_MARSHALLER);
  private static final Key<byte[]> HEADER3_KEY_BIN =
      Key.of("header3-bin", Metadata.BINARY_BYTE_MARSHALLER);

  @Captor private ArgumentCaptor<Map<String, String>> propertiesCaptor;

  @Test
  @DisplayName("Should propagate gRPC string headers as connection properties")
  public void propagateStringHeaders() throws UnauthorizedException {
    // Given
    StargateGrpc.StargateBlockingStub stub =
        makeBlockingStubWithClientHeaders(
                headers -> {
                  headers.put(HEADER1_KEY, "value1");
                  headers.put(HEADER2_KEY, "value2");
                  headers.put(HEADER3_KEY_BIN, new byte[] {});
                })
            .withCallCredentials(new StargateBearerToken("token"));
    mockAnyQueryAsVoid();
    when(persistence.newConnection(any())).thenReturn(connection);

    AuthenticatedUser authenticatedUser = mock(AuthenticatedUser.class);

    AuthenticationSubject authenticationSubject = mock(AuthenticationSubject.class);
    when(authenticationSubject.asUser()).thenReturn(authenticatedUser);

    AuthenticationService authenticationService = mock(AuthenticationService.class);
    when(authenticationService.validateToken(anyString(), any(Map.class)))
        .thenReturn(authenticationSubject);

    startServer(new NewConnectionInterceptor(persistence, authenticationService, "mockAdminToken"));

    // When
    executeQuery(stub, "mock query");

    // Then
    verify(connection).setCustomProperties(propertiesCaptor.capture());
    assertThat(propertiesCaptor.getValue())
        .containsEntry(HEADER1_KEY.name(), "value1")
        .containsEntry(HEADER2_KEY.name(), "value2")
        .doesNotContainKey(HEADER3_KEY_BIN.name());
  }

  private void mockAnyQueryAsVoid() {
    Result.Prepared prepared =
        new Result.Prepared(
            Utils.STATEMENT_ID,
            Utils.RESULT_METADATA_ID,
            Utils.makeResultMetadata(),
            Utils.makePreparedMetadata(),
            true,
            false);
    when(connection.prepare(any(String.class), any(Parameters.class)))
        .thenReturn(CompletableFuture.completedFuture(prepared));
    when(connection.execute(any(Statement.class), any(Parameters.class), anyLong()))
        .thenReturn(CompletableFuture.completedFuture(new Result.Void()));
  }
}
