package io.stargate.grpc.codec.cql;

import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.QueryOuterClass.Value.InnerCase;
import java.nio.ByteBuffer;

public class SmallintCodec implements ValueCodec {

  @Override
  public ByteBuffer encode(@NonNull Value value, @NonNull ColumnType type) {
    if (value.getInnerCase() != InnerCase.INT) {
      throw new IllegalArgumentException("Expected integer type");
    }
    short shortValue = (short) value.getInt();
    if (shortValue != value.getInt()) {
      throw new IllegalArgumentException(
          String.format("Valid range for smallint is %d to %d", Short.MIN_VALUE, Short.MAX_VALUE));
    }
    return TypeCodecs.SMALLINT.encodePrimitive(shortValue, PROTOCOL_VERSION);
  }

  @Override
  public Value decode(@NonNull ByteBuffer bytes, @NonNull ColumnType type) {
    return Value.newBuilder()
        .setInt(TypeCodecs.SMALLINT.decodePrimitive(bytes, PROTOCOL_VERSION))
        .build();
  }
}
