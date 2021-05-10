package io.stargate.grpc.codec.cql;

import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.QueryOuterClass.Value.InnerCase;
import java.nio.ByteBuffer;

public class TimeCodec implements ValueCodec {

  @Override
  public ByteBuffer encode(@NonNull Value value, @NonNull ColumnType type) {
    if (value.getInnerCase() != InnerCase.TIME) {
      throw new IllegalArgumentException("Expected time type");
    }
    long time = value.getTime();
    if (time < 0 || time > 86399999999999L) {
      throw new IllegalArgumentException("Valid range for time is 0 to 86399999999999 nanoseconds");
    }
    return TypeCodecs.BIGINT.encodePrimitive(time, PROTOCOL_VERSION);
  }

  @Override
  public Value decode(@NonNull ByteBuffer bytes) {
    return Value.newBuilder()
        .setTime(TypeCodecs.BIGINT.decodePrimitive(bytes, PROTOCOL_VERSION))
        .build();
  }
}
