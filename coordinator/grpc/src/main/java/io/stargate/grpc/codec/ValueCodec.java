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
package io.stargate.grpc.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.grpc.Values;
import io.stargate.proto.QueryOuterClass.Value;
import java.nio.ByteBuffer;

/** Codec to convert to/from gRPC and CQL native protocol ({@link ByteBuffer}) values. */
public interface ValueCodec {
  ProtocolVersion PROTOCOL_VERSION = defaultProtocolVersion();

  /**
   * Convert a gRPC tagged-union payload value into the internal CQL native protocol representation.
   *
   * @param value A tagged-union payload value representing most CQL support types.
   * @param type The type value being converted. This is for determining sub-types for composites
   *     like lists, sets, maps, tuples and UDTs.
   * @return The value converted to its CQL native protocol representation.
   */
  ByteBuffer encode(@NonNull Value value, @NonNull ColumnType type);

  /**
   * Convert a CQL native protocol represented value into a gRPC tagged-union value.
   *
   * @param bytes The bytes for the CQL native protocol value.
   * @param type The type value being converted. This is for determining sub-types for composites
   *     like lists, sets, maps, tuples and UDTs.
   * @return A gRPC tagged-union payload value.
   */
  Value decode(@NonNull ByteBuffer bytes, @NonNull ColumnType type);

  static ByteBuffer encodeValue(ValueCodec codec, Value value, ColumnType columnType) {
    if (value.hasNull()) {
      return null;
    } else {
      return codec.encode(value, columnType);
    }
  }

  static Value decodeValue(ValueCodec codec, ByteBuffer bytes, ColumnType columnType) {
    if (bytes == null) {
      return Values.NULL;
    } else {
      return codec.decode(bytes, columnType);
    }
  }

  /**
   * Calculates the driver's default protocol version using Stargate's {@link ProtocolVersion} type.
   * Note: Use the constant {@link ValueCodec#PROTOCOL_VERSION} instead of using this directly.
   *
   * @return The default protocol version supported by Stargate.
   */
  static ProtocolVersion defaultProtocolVersion() {
    return org.apache.cassandra.stargate.transport.ProtocolVersion.CURRENT.toDriverVersion();
  }
}
