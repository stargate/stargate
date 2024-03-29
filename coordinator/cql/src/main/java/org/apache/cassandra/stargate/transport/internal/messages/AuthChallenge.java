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
import java.nio.ByteBuffer;
import org.apache.cassandra.stargate.transport.ProtocolVersion;
import org.apache.cassandra.stargate.transport.internal.CBUtil;
import org.apache.cassandra.stargate.transport.internal.Message;

/** SASL challenge sent from client to server */
public class AuthChallenge extends Message.Response {
  public static final Message.Codec<AuthChallenge> codec =
      new Message.Codec<AuthChallenge>() {
        @Override
        public AuthChallenge decode(ByteBuf body, ProtocolVersion version) {
          ByteBuffer b = CBUtil.readValue(body);
          byte[] token = new byte[b.remaining()];
          b.get(token);
          return new AuthChallenge(token);
        }

        @Override
        public void encode(AuthChallenge challenge, ByteBuf dest, ProtocolVersion version) {
          CBUtil.writeValue(challenge.token, dest);
        }

        @Override
        public int encodedSize(AuthChallenge challenge, ProtocolVersion version) {
          return CBUtil.sizeOfValue(challenge.token);
        }
      };

  private final byte[] token;

  public AuthChallenge(byte[] token) {
    super(Message.Type.AUTH_CHALLENGE);
    this.token = token;
  }

  public byte[] getToken() {
    return token;
  }
}
