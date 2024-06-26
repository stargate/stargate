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
import io.stargate.db.Authenticator;
import io.stargate.db.DriverInfo;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.cassandra.stargate.transport.ProtocolException;
import org.apache.cassandra.stargate.transport.ProtocolVersion;
import org.apache.cassandra.stargate.transport.internal.CBUtil;
import org.apache.cassandra.stargate.transport.internal.Message;
import org.apache.cassandra.stargate.transport.internal.frame.checksum.ChecksummingTransformer;
import org.apache.cassandra.stargate.transport.internal.frame.compress.CompressingTransformer;
import org.apache.cassandra.stargate.transport.internal.frame.compress.Compressor;
import org.apache.cassandra.stargate.transport.internal.frame.compress.LZ4Compressor;
import org.apache.cassandra.stargate.transport.internal.frame.compress.SnappyCompressor;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.ChecksumType;

/** The initial message of the protocol. Sets up a number of connection options. */
public class StartupMessage extends Message.Request {
  public static final String CQL_VERSION = "CQL_VERSION";
  public static final String COMPRESSION = "COMPRESSION";
  public static final String PROTOCOL_VERSIONS = "PROTOCOL_VERSIONS";
  public static final String DRIVER_NAME = "DRIVER_NAME";
  public static final String DRIVER_VERSION = "DRIVER_VERSION";
  public static final String CHECKSUM = "CONTENT_CHECKSUM";
  public static final String THROW_ON_OVERLOAD = "THROW_ON_OVERLOAD";

  public static final Message.Codec<StartupMessage> codec =
      new Message.Codec<StartupMessage>() {
        @Override
        public StartupMessage decode(ByteBuf body, ProtocolVersion version) {
          return new StartupMessage(upperCaseKeys(CBUtil.readStringMap(body)));
        }

        @Override
        public void encode(StartupMessage msg, ByteBuf dest, ProtocolVersion version) {
          CBUtil.writeStringMap(msg.options, dest);
        }

        @Override
        public int encodedSize(StartupMessage msg, ProtocolVersion version) {
          return CBUtil.sizeOfStringMap(msg.options);
        }
      };

  public final Map<String, String> options;

  public StartupMessage(Map<String, String> options) {
    super(Message.Type.STARTUP);
    this.options = options;
  }

  @Override
  protected CompletableFuture<? extends Response> execute(long queryStartNanoTime) {
    String cqlVersion = options.get(CQL_VERSION);
    if (cqlVersion == null)
      throw new ProtocolException("Missing value CQL_VERSION in STARTUP message");

    try {
      if (new CassandraVersion(cqlVersion).compareTo(new CassandraVersion("2.99.0")) < 0)
        throw new ProtocolException(
            String.format(
                "CQL version %s is not supported by the binary protocol (supported version are >= 3.0.0)",
                cqlVersion));
    } catch (IllegalArgumentException e) {
      throw new ProtocolException(e.getMessage());
    }

    ChecksumType checksumType = getChecksumType();
    Compressor compressor = getCompressor();

    if (null != checksumType) {
      if (!connection.getVersion().supportsChecksums())
        throw new ProtocolException(
            String.format(
                "Invalid message flag. Protocol version %s does not support frame body checksums",
                connection.getVersion().toString()));
      connection.setTransformer(ChecksummingTransformer.getTransformer(checksumType, compressor));
    } else if (null != compressor) {
      connection.setTransformer(CompressingTransformer.getTransformer(compressor));
    }

    connection.setThrowOnOverload("1".equals(options.get(THROW_ON_OVERLOAD)));

    String driverName = options.get(DRIVER_NAME);
    if (null != driverName) {
      clientInfo().registerDriverInfo(DriverInfo.of(driverName, options.get(DRIVER_VERSION)));
      connection.getConnectionMetrics().updateDriverInfo(clientInfo());
    }

    Authenticator authenticator = persistence().getAuthenticator();
    if (authenticator.requireAuthentication())
      return CompletableFuture.completedFuture(
          new AuthenticateMessage(authenticator.getInternalClassName()));
    else return CompletableFuture.completedFuture(new ReadyMessage());
  }

  private static Map<String, String> upperCaseKeys(Map<String, String> options) {
    Map<String, String> newMap = new HashMap<String, String>(options.size());
    for (Map.Entry<String, String> entry : options.entrySet())
      newMap.put(entry.getKey().toUpperCase(), entry.getValue());
    return newMap;
  }

  private ChecksumType getChecksumType() throws ProtocolException {
    String name = options.get(CHECKSUM);
    try {
      return name != null ? ChecksumType.valueOf(name.toUpperCase()) : null;
    } catch (IllegalArgumentException e) {
      throw new ProtocolException(
          String.format(
              "Requested checksum type %s is not known or supported by "
                  + "this version of Cassandra",
              name));
    }
  }

  private Compressor getCompressor() throws ProtocolException {
    String name = options.get(COMPRESSION);
    if (null == name) return null;

    switch (name.toLowerCase()) {
      case "snappy":
        {
          if (SnappyCompressor.INSTANCE == null)
            throw new ProtocolException("This instance does not support Snappy compression");

          return SnappyCompressor.INSTANCE;
        }
      case "lz4":
        return LZ4Compressor.INSTANCE;
      default:
        throw new ProtocolException(String.format("Unknown compression algorithm: %s", name));
    }
  }

  @Override
  public String toString() {
    return "STARTUP " + options;
  }
}
