package io.stargate.grpc.codec.cql;

import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.QueryOuterClass.Value.InnerCase;
import java.nio.ByteBuffer;

public class FloatCodec implements ValueCodec {

  @Override
  public ByteBuffer encode(@NonNull Value value, @NonNull ColumnType type) {
    if (value.getInnerCase() != InnerCase.FLOAT) {
      throw new IllegalArgumentException("Expected float type");
    }
    return TypeCodecs.FLOAT.encodePrimitive(value.getFloat(), PROTOCOL_VERSION);
  }

  @Override
  public Value decode(@NonNull ByteBuffer bytes) {
    return Value.newBuilder()
        .setFloat(TypeCodecs.FLOAT.decodePrimitive(bytes, PROTOCOL_VERSION))
        .build();
  }
}
