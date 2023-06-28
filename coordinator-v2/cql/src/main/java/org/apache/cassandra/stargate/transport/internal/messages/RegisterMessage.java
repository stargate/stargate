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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.cassandra.stargate.transport.ProtocolException;
import org.apache.cassandra.stargate.transport.ProtocolVersion;
import org.apache.cassandra.stargate.transport.internal.CBUtil;
import org.apache.cassandra.stargate.transport.internal.Connection;
import org.apache.cassandra.stargate.transport.internal.CqlServer;
import org.apache.cassandra.stargate.transport.internal.Event;
import org.apache.cassandra.stargate.transport.internal.Message;
import org.apache.cassandra.stargate.transport.internal.ServerConnection;

public class RegisterMessage extends Message.Request {
  public static final Message.Codec<RegisterMessage> codec =
      new Message.Codec<RegisterMessage>() {
        @Override
        public RegisterMessage decode(ByteBuf body, ProtocolVersion version) {
          int length = body.readUnsignedShort();
          List<Event.Type> eventTypes = new ArrayList<>(length);
          for (int i = 0; i < length; ++i)
            eventTypes.add(CBUtil.readEnumValue(Event.Type.class, body));
          return new RegisterMessage(eventTypes);
        }

        @Override
        public void encode(RegisterMessage msg, ByteBuf dest, ProtocolVersion version) {
          dest.writeShort(msg.eventTypes.size());
          for (Event.Type type : msg.eventTypes) CBUtil.writeEnumValue(type, dest);
        }

        @Override
        public int encodedSize(RegisterMessage msg, ProtocolVersion version) {
          int size = 2;
          for (Event.Type type : msg.eventTypes) size += CBUtil.sizeOfEnumValue(type);
          return size;
        }
      };

  public final List<Event.Type> eventTypes;

  public RegisterMessage(List<Event.Type> eventTypes) {
    super(Message.Type.REGISTER);
    this.eventTypes = eventTypes;
  }

  @Override
  protected CompletableFuture<? extends Response> execute(long queryStartNanoTime) {
    assert connection instanceof ServerConnection;
    Connection.Tracker tracker = connection.getTracker();
    assert tracker instanceof CqlServer.ConnectionTracker;
    for (Event.Type type : eventTypes) {
      if (type.minimumVersion.isGreaterThan(connection.getVersion()))
        throw new ProtocolException(
            "Event " + type.name() + " not valid for protocol version " + connection.getVersion());
      ((CqlServer.ConnectionTracker) tracker).register(type, connection().channel());
    }
    return CompletableFuture.completedFuture(new ReadyMessage());
  }

  @Override
  public String toString() {
    return "REGISTER " + eventTypes;
  }
}
