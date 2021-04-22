package io.stargate.grpc.codec.cql;

import edu.umd.cs.findbugs.annotations.NonNull;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.proto.QueryOuterClass.Value;
import java.nio.ByteBuffer;

public interface ValueCodec {
  ByteBuffer encode(@NonNull Value value, @NonNull ColumnType type);

  Value decode(@NonNull ByteBuffer bytes);
}
