package io.stargate.grpc.codec.cql;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.grpc.Status;
import io.grpc.StatusException;
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
  public ByteBuffer encode(@NonNull QueryOuterClass.Value value, @NotNull Column.ColumnType type)
      throws StatusException {
    if (value.getInnerCase() != InnerCase.UUID) {
      throw Status.INVALID_ARGUMENT.withDescription("Expected UUID type").asException();
    }
    UUID uuid = new UUID(value.getUuid().getMsb(), value.getUuid().getLsb());
    if ((isTimeVersion() && uuid.version() != 1) || uuid.version() != 4) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Invalid UUID version. Expected version %d, but received version %d",
                  isTimeVersion() ? 1 : 4, uuid.version()))
          .asException();
    }
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

  private boolean isTimeVersion() {
    return innerCodec == TypeCodecs.TIMEUUID;
  }
}
