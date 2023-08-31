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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.cassandra.stargate.transport.ProtocolVersion;
import org.apache.cassandra.stargate.transport.internal.Message;
import org.apache.cassandra.stargate.transport.internal.frame.compress.SnappyCompressor;
import org.apache.cassandra.utils.ChecksumType;

/** Message to indicate that the server is ready to receive requests. */
public class OptionsMessage extends Message.Request {
  public static final Message.Codec<OptionsMessage> codec =
      new Message.Codec<OptionsMessage>() {
        @Override
        public OptionsMessage decode(ByteBuf body, ProtocolVersion version) {
          return new OptionsMessage();
        }

        @Override
        public void encode(OptionsMessage msg, ByteBuf dest, ProtocolVersion version) {}

        @Override
        public int encodedSize(OptionsMessage msg, ProtocolVersion version) {
          return 0;
        }
      };

  public OptionsMessage() {
    super(Message.Type.OPTIONS);
  }

  @Override
  protected CompletableFuture<? extends Response> execute(long queryStartNanoTime) {

    List<String> compressions = new ArrayList<>();
    if (SnappyCompressor.INSTANCE != null) compressions.add("snappy");
    // LZ4 is always available since worst case scenario it default to a pure JAVA implem.
    compressions.add("lz4");

    Map<String, List<String>> supported = new HashMap<>(persistence().cqlSupportedOptions());
    assert supported.containsKey(StartupMessage.CQL_VERSION);

    supported.put(StartupMessage.COMPRESSION, compressions);
    supported.put(StartupMessage.PROTOCOL_VERSIONS, ProtocolVersion.supportedVersions());

    if (connection.getVersion().supportsChecksums()) {
      ChecksumType[] types = ChecksumType.values();
      List<String> checksumImpls = new ArrayList<>(types.length);
      for (ChecksumType type : types) checksumImpls.add(type.toString());
      supported.put(StartupMessage.CHECKSUM, checksumImpls);
    }

    return CompletableFuture.completedFuture(new SupportedMessage(supported));
  }

  @Override
  public String toString() {
    return "OPTIONS";
  }
}
