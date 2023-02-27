/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.stargate.transport.internal.messages;

import io.netty.buffer.ByteBuf;
import io.stargate.db.AuthenticatedUser;
import io.stargate.db.Authenticator;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import org.apache.cassandra.stargate.exceptions.AuthenticationException;
import org.apache.cassandra.stargate.transport.ProtocolException;
import org.apache.cassandra.stargate.transport.ProtocolVersion;
import org.apache.cassandra.stargate.transport.internal.CBUtil;
import org.apache.cassandra.stargate.transport.internal.Message;
import org.apache.cassandra.stargate.transport.internal.ServerConnection;

/**
 * A SASL token message sent from client to server. Some SASL mechanisms and clients may send an
 * initial token before receiving a challenge from the server.
 */
public class AuthResponse extends Message.Request {
  public static final Message.Codec<AuthResponse> codec =
      new Message.Codec<AuthResponse>() {
        @Override
        public AuthResponse decode(ByteBuf body, ProtocolVersion version) {
          if (version == ProtocolVersion.V1)
            throw new ProtocolException(
                "SASL Authentication is not supported in version 1 of the protocol");

          ByteBuffer b = CBUtil.readValue(body);
          byte[] token = new byte[b.remaining()];
          b.get(token);
          return new AuthResponse(token);
        }

        @Override
        public void encode(AuthResponse response, ByteBuf dest, ProtocolVersion version) {
          CBUtil.writeValue(response.token, dest);
        }

        @Override
        public int encodedSize(AuthResponse response, ProtocolVersion version) {
          return CBUtil.sizeOfValue(response.token);
        }
      };

  private final byte[] token;

  public AuthResponse(byte[] token) {
    super(Message.Type.AUTH_RESPONSE);
    assert token != null;
    this.token = token;
  }

  @Override
  protected CompletableFuture<? extends Response> execute(long queryStartNanoTime) {
    CompletableFuture<Response> future = new CompletableFuture<>();
    persistence()
        .executeAuthResponse(
            () -> {
              try {
                Authenticator.SaslNegotiator negotiator =
                    ((ServerConnection) connection).getSaslNegotiator();
                byte[] challenge = negotiator.evaluateResponse(token);
                if (negotiator.isComplete()) {
                  AuthenticatedUser authenticatedUser = negotiator.getAuthenticatedUser();
                  persistenceConnection().login(authenticatedUser);
                  if (authenticatedUser.token() != null) {
                    ((ServerConnection) connection)
                        .clientInfo()
                        .setAuthenticatedUser(authenticatedUser);
                  }

                  connection.getConnectionMetrics().markAuthSuccess();

                  // authentication is complete, send a ready message to the client
                  future.complete(new AuthSuccess(challenge));
                } else {
                  future.complete(new AuthChallenge(challenge));
                }
              } catch (AuthenticationException ae) {
                connection.getConnectionMetrics().markAuthFailure();
                future.complete(ErrorMessage.fromException(ae));
              } catch (Exception e) {
                connection.getConnectionMetrics().markAuthError();
                future.completeExceptionally(e);
              }
            });
    return future;
  }
}
