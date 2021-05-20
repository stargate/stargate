package io.stargate.grpc.codec.cql;

import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.QueryOuterClass.Value.InnerCase;
import java.nio.ByteBuffer;

public class BigintCodec implements ValueCodec {

  @Override
  public ByteBuffer encode(@NonNull Value value, @NonNull ColumnType type) {
    if (value.getInnerCase() != InnerCase.INT) {
      throw new IllegalArgumentException("Expected integer type");
    }
    return TypeCodecs.BIGINT.encodePrimitive(value.getInt(), PROTOCOL_VERSION);
  }

  @Override
  public Value decode(@NonNull ByteBuffer bytes, @NonNull ColumnType type) {
    return Value.newBuilder()
        .setInt(TypeCodecs.BIGINT.decodePrimitive(bytes, PROTOCOL_VERSION))
        .build();
  }
}
