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

import com.codahale.metrics.Counter;
import io.netty.channel.Channel;
import io.stargate.db.Persistence;
import org.apache.cassandra.stargate.metrics.ConnectionMetrics;
import org.apache.cassandra.stargate.transport.ProtocolVersion;

public abstract class ServerConnection extends Connection {

  private final Persistence persistence;
  protected volatile ConnectionStage stage;
  public final Counter requests = new Counter();

  protected ServerConnection(
      Channel channel,
      ProtocolVersion version,
      Tracker tracker,
      ConnectionMetrics connectionMetrics,
      Persistence persistence) {
    super(channel, version, tracker, connectionMetrics);
    this.persistence = persistence;
  }

  Persistence persistence() {
    return persistence;
  }

  ConnectionStage stage() {
    return stage;
  }

  abstract void validateNewMessage(Message.Type type, ProtocolVersion version);

  abstract void applyStateTransition(Message.Type requestType, Message.Type responseType);
}
