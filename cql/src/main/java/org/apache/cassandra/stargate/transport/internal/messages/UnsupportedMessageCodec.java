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
import org.apache.cassandra.stargate.transport.ProtocolException;
import org.apache.cassandra.stargate.transport.ProtocolVersion;
import org.apache.cassandra.stargate.transport.internal.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Catch-all codec for any unsupported legacy messages. */
public class UnsupportedMessageCodec<T extends Message> implements Message.Codec<T> {
  public static final UnsupportedMessageCodec instance = new UnsupportedMessageCodec();

  private static final Logger logger = LoggerFactory.getLogger(UnsupportedMessageCodec.class);

  @Override
  public T decode(ByteBuf body, ProtocolVersion version) {
    if (ProtocolVersion.SUPPORTED.contains(version)) {
      logger.error("Received invalid message for supported protocol version {}", version);
    }
    throw new ProtocolException("Unsupported message");
  }

  @Override
  public void encode(T t, ByteBuf dest, ProtocolVersion version) {
    throw new ProtocolException("Unsupported message");
  }

  @Override
  public int encodedSize(T t, ProtocolVersion version) {
    return 0;
  }
}
