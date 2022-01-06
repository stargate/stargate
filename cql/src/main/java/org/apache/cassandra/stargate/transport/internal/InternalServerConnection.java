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
package org.apache.cassandra.stargate.transport.internal;

import io.micrometer.core.instrument.Tags;
import io.netty.channel.Channel;
import io.stargate.db.Persistence;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.cassandra.stargate.metrics.ConnectionMetrics;
import org.apache.cassandra.stargate.transport.ProtocolException;
import org.apache.cassandra.stargate.transport.ProtocolVersion;

/**
 * An "internal" connection from another Stargate service (REST, GraphQL, etc).
 *
 * <p>In this mode, there is no initial authentication. Instead, each message carries its own token
 * (and possibly tenant ID) in its custom payload.
 *
 * <p>Persistence backends do not support per-message authentication, therefore it is not possible
 * to associate this object with a single persistence connection. Instead, it is retrieved from a
 * factory for each message.
 */
public class InternalServerConnection extends ServerConnection {

  private final PersistenceConnectionFactory persistenceConnectionFactory;

  public InternalServerConnection(Channel channel, ProtocolVersion version, CqlServer server) {
    super(channel, version, server.connectionTracker, NOOP_CONNECTION_METRICS, server.persistence);
    this.persistenceConnectionFactory = server.persistenceConnectionFactory;
    this.stage = ConnectionStage.ESTABLISHED;
  }

  public Persistence.Connection persistenceConnection(Map<String, ByteBuffer> customPayload) {
    return persistenceConnectionFactory.newConnection(customPayload);
  }

  @Override
  void validateNewMessage(Message.Type type, ProtocolVersion version) {
    switch (stage) {
      case ESTABLISHED:
        if (type != Message.Type.STARTUP && type != Message.Type.OPTIONS) {
          throw new ProtocolException(
              String.format("Unexpected message %s, expecting STARTUP or OPTIONS", type));
        }
        break;
      case READY:
        if (type == Message.Type.STARTUP) {
          throw new ProtocolException(
              "Unexpected message STARTUP, the connection is already initialized");
        }
        break;
      default:
        throw new AssertionError();
    }
  }

  @Override
  void applyStateTransition(Message.Type requestType, Message.Type responseType) {
    switch (stage) {
      case ESTABLISHED:
        if (requestType == Message.Type.STARTUP) {
          if (responseType == Message.Type.AUTHENTICATE) {
            throw new AssertionError("Unexpected CQL auth on internal connection");
          } else if (responseType == Message.Type.READY) {
            stage = ConnectionStage.READY;
          }
        }
        break;
      case READY:
        break;
      default:
        throw new AssertionError();
    }
  }

  // Don't track metrics for this connection (assuming that the client service has its own metrics
  // on its side)
  private static final ConnectionMetrics NOOP_CONNECTION_METRICS =
      new ConnectionMetrics() {
        @Override
        public Tags getTags() {
          return Tags.empty();
        }

        @Override
        public void markRequestProcessed() {
          // intentionally empty
        }

        @Override
        public void markRequestDiscarded() {
          // intentionally empty
        }

        @Override
        public void markAuthSuccess() {
          // intentionally empty
        }

        @Override
        public void markAuthFailure() {
          // intentionally empty
        }

        @Override
        public void markAuthError() {
          // intentionally empty
        }
      };
}
