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
package io.stargate.grpc.codec.cql;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.proto.QueryOuterClass.Value;
import java.nio.ByteBuffer;

/** Codec to convert to/from gRPC and CQL native protocol ({@link ByteBuffer}) values. */
public interface ValueCodec {
  ProtocolVersion PROTOCOL_VERSION = ProtocolVersion.DEFAULT;

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
   * @return A gRPC tagged-union payload value.
   */
  Value decode(@NonNull ByteBuffer bytes);
}
