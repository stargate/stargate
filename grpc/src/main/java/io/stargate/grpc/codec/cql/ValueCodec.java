package io.stargate.grpc.codec.cql;

import edu.umd.cs.findbugs.annotations.NonNull;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.proto.QueryOuterClass.Value;
import java.nio.ByteBuffer;

/** Codec to convert to/from gRPC and CQL native protocol ({@link ByteBuffer}) values. */
public interface ValueCodec {
  /**
   * @param value
   * @param type
   * @return
   */
  ByteBuffer encode(@NonNull Value value, @NonNull ColumnType type);

  /**
   * @param bytes
   * @return
   */
  Value decode(@NonNull ByteBuffer bytes);
}
