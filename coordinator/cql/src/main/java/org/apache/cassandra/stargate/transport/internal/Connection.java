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

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;
import org.apache.cassandra.stargate.metrics.ConnectionMetrics;
import org.apache.cassandra.stargate.transport.ProtocolVersion;

public class Connection {
  static final AttributeKey<Connection> attributeKey = AttributeKey.valueOf("CONN");

  private final Channel channel;
  private final ProtocolVersion version;
  private final Tracker tracker;
  private final ConnectionMetrics connectionMetrics;
  // Cassandra {4.0.10}
  private volatile Compressor preV5MessageCompressor;
  private boolean throwOnOverload;

  public Connection(
      Channel channel,
      ProtocolVersion version,
      Tracker tracker,
      ConnectionMetrics connectionMetrics) {
    this.channel = channel;
    this.version = version;
    this.tracker = tracker;
    this.connectionMetrics = connectionMetrics;

    tracker.addConnection(channel, this);
  }

  // Cassandra {4.0.10}
  public void setCompressor(Compressor compressor) {
    this.preV5MessageCompressor = compressor;
  }

  public Compressor getCompressor() {
    return preV5MessageCompressor;
  }

  public void setThrowOnOverload(boolean throwOnOverload) {
    this.throwOnOverload = throwOnOverload;
  }

  public boolean isThrowOnOverload() {
    return throwOnOverload;
  }

  public Tracker getTracker() {
    return tracker;
  }

  public ConnectionMetrics getConnectionMetrics() {
    return connectionMetrics;
  }

  public ProtocolVersion getVersion() {
    return version;
  }

  public Channel channel() {
    return channel;
  }

  public interface Factory {
    Connection newConnection(Channel channel, ProxyInfo info, ProtocolVersion version);
  }

  public interface Tracker {
    void addConnection(Channel ch, Connection connection);
  }
}
