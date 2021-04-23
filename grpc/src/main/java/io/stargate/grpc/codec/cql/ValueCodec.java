package io.stargate.grpc.codec.cql;

import edu.umd.cs.findbugs.annotations.NonNull;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.proto.QueryOuterClass.Value;
import java.nio.ByteBuffer;

/** Codec to convert to/from gRPC and CQL native protocol ({@link ByteBuffer}) values. */
public interface ValueCodec {
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
