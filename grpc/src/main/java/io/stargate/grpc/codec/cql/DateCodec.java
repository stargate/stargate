package io.stargate.grpc.codec.cql;

import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.QueryOuterClass.Value.InnerCase;
import java.nio.ByteBuffer;

public class DateCodec implements ValueCodec {

  @Override
  public ByteBuffer encode(@NonNull Value value, @NonNull ColumnType type) {
    if (value.getInnerCase() != InnerCase.DATE) {
      throw new IllegalArgumentException("Expected date type");
    }
    return TypeCodecs.INT.encodePrimitive(value.getDate(), PROTOCOL_VERSION);
  }

  @Override
  public Value decode(@NonNull ByteBuffer bytes) {
    return Value.newBuilder()
        .setDate(TypeCodecs.INT.decodePrimitive(bytes, PROTOCOL_VERSION))
        .build();
  }
}
