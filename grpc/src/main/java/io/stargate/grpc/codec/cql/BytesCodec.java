package io.stargate.grpc.codec.cql;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusException;
import io.stargate.db.schema.Column;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.QueryOuterClass.Value.InnerCase;
import java.nio.ByteBuffer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class BytesCodec implements ValueCodec {
  @Nullable
  @Override
  public ByteBuffer encode(@Nullable QueryOuterClass.Value value, @NotNull Column.ColumnType type)
      throws StatusException {
    if (value.getInnerCase() != InnerCase.BYTES) {
      throw Status.FAILED_PRECONDITION.withDescription("Expected bytes type").asException();
    }
    return ByteBuffer.wrap(value.getBytes().toByteArray());
  }

  @NotNull
  @Override
  public QueryOuterClass.Value decode(@Nullable ByteBuffer bytes) throws StatusException {
    return Value.newBuilder().setBytes(ByteString.copyFrom(bytes)).build();
  }
}
