package io.stargate.grpc.codec.cql;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.stargate.db.schema.Column;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.QueryOuterClass.Uuid;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.QueryOuterClass.Value.InnerCase;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.jetbrains.annotations.NotNull;

public class UuidCodec implements ValueCodec {
  private TypeCodec<UUID> innerCodec;

  public UuidCodec(@NonNull TypeCodec<UUID> innerCodec) {
    this.innerCodec = innerCodec;
  }

  @Override
  public ByteBuffer encode(@NonNull QueryOuterClass.Value value, @NotNull Column.ColumnType type) {
    if (value.getInnerCase() != InnerCase.UUID) {
      throw new IllegalArgumentException("Expected UUID type");
    }
    UUID uuid = new UUID(value.getUuid().getMsb(), value.getUuid().getLsb());
    return innerCodec.encode(uuid, ProtocolVersion.DEFAULT);
  }

  @Override
  public QueryOuterClass.Value decode(@NonNull ByteBuffer bytes) {
    UUID uuid = innerCodec.decode(bytes, ProtocolVersion.DEFAULT);
    assert uuid != null;
    return Value.newBuilder()
        .setUuid(
            Uuid.newBuilder()
                .setMsb(uuid.getMostSignificantBits())
                .setLsb(uuid.getLeastSignificantBits())
                .build())
        .build();
  }
}
