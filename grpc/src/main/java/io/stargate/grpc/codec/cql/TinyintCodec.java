package io.stargate.grpc.codec.cql;

import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.QueryOuterClass.Value.InnerCase;
import java.nio.ByteBuffer;

public class TinyintCodec implements ValueCodec {

  @Override
  public ByteBuffer encode(@NonNull Value value, @NonNull ColumnType type) {
    if (value.getInnerCase() != InnerCase.INT) {
      throw new IllegalArgumentException("Expected integer type");
    }
    byte byteValue = (byte) value.getInt();
    if (byteValue != value.getInt()) {
      throw new IllegalArgumentException(
          String.format("Valid range for tinyint is %d to %d", Byte.MIN_VALUE, Byte.MAX_VALUE));
    }
    return TypeCodecs.TINYINT.encodePrimitive(byteValue, PROTOCOL_VERSION);
  }

  @Override
  public Value decode(@NonNull ByteBuffer bytes) {
    return Value.newBuilder()
        .setInt(TypeCodecs.TINYINT.decodePrimitive(bytes, PROTOCOL_VERSION))
        .build();
  }
}
