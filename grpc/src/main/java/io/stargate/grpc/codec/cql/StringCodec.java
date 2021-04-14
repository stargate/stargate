package io.stargate.grpc.codec.cql;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.stargate.db.schema.Column;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.QueryOuterClass.Value.InnerCase;
import java.nio.ByteBuffer;

public class StringCodec implements ValueCodec {
  private TypeCodec<String> innerCodec;

  public StringCodec(@NonNull TypeCodec<String> innerCodec) {
    this.innerCodec = innerCodec;
  }

  @Nullable
  @Override
  public ByteBuffer encode(@Nullable QueryOuterClass.Value value, @NonNull Column.ColumnType type) {
    if (value.getInnerCase() != InnerCase.STRING) {
      throw new IllegalArgumentException("Expected string type");
    }
    return innerCodec.encode(value.getString(), ProtocolVersion.DEFAULT);
  }

  @NonNull
  @Override
  public QueryOuterClass.Value decode(@Nullable ByteBuffer bytes) {
    return Value.newBuilder().setString(innerCodec.decode(bytes, ProtocolVersion.DEFAULT)).build();
  }
}
