package io.stargate.grpc.codec.cql;

import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.QueryOuterClass.Value.InnerCase;
import java.nio.ByteBuffer;

public class BooleanCodec implements ValueCodec {

  @Override
  public ByteBuffer encode(@NonNull Value value, @NonNull ColumnType type) {
    if (value.getInnerCase() != InnerCase.BOOLEAN) {
      throw new IllegalArgumentException("Expected boolean type");
    }
    return TypeCodecs.BOOLEAN.encodePrimitive(value.getBoolean(), PROTOCOL_VERSION);
  }

  @Override
  public Value decode(@NonNull ByteBuffer bytes) {
    return Value.newBuilder()
        .setBoolean(TypeCodecs.BOOLEAN.decodePrimitive(bytes, PROTOCOL_VERSION))
        .build();
  }
}
