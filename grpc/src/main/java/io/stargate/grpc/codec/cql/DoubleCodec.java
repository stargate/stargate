package io.stargate.grpc.codec.cql;

import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.QueryOuterClass.Value.InnerCase;
import java.nio.ByteBuffer;

public class DoubleCodec implements ValueCodec {
  @Override
  public ByteBuffer encode(@NonNull Value value, @NonNull ColumnType type) {
    if (value.getInnerCase() != InnerCase.DOUBLE) {
      throw new IllegalArgumentException("Expected double type");
    }
    return TypeCodecs.DOUBLE.encodePrimitive(value.getDouble(), PROTOCOL_VERSION);
  }

  @Override
  public Value decode(@NonNull ByteBuffer bytes, @NonNull ColumnType type) {
    return Value.newBuilder()
        .setDouble(TypeCodecs.DOUBLE.decodePrimitive(bytes, PROTOCOL_VERSION))
        .build();
  }
}
