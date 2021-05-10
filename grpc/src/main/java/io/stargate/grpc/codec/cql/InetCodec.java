package io.stargate.grpc.codec.cql;

import com.google.protobuf.ByteString;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.QueryOuterClass.Value.InnerCase;
import java.nio.ByteBuffer;

public class InetCodec implements ValueCodec {

  @Override
  public ByteBuffer encode(@NonNull Value value, @NonNull ColumnType type) {
    if (value.getInnerCase() != InnerCase.BYTES) {
      throw new IllegalArgumentException("Expected bytes type");
    }
    ByteString address = value.getBytes();
    int size = address.size();
    if (size != 4 && size != 16) {
      throw new IllegalArgumentException("Expected 4 or 16 bytes for an IPv4 or IPv6 address");
    }
    return ByteBuffer.wrap(value.getBytes().toByteArray());
  }

  @Override
  public Value decode(@NonNull ByteBuffer bytes) {
    return Value.newBuilder().setBytes(ByteString.copyFrom(bytes)).build();
  }
}
