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
import org.jetbrains.annotations.Nullable;

public class UuidCodec implements ValueCodec {
  private TypeCodec<UUID> innerCodec;

  public UuidCodec(@NonNull TypeCodec<UUID> innerCodec) {
    this.innerCodec = innerCodec;
  }

  @Nullable
  @Override
  public ByteBuffer encode(@Nullable QueryOuterClass.Value value, @NotNull Column.ColumnType type)
      throws StatusException {
    if (value.getInnerCase() != InnerCase.UUID) {
      throw Status.FAILED_PRECONDITION.withDescription("Expected UUID type").asException();
    }
    UUID uuid = new UUID(value.getUuid().getMsb(), value.getUuid().getLsb());
    if ((isTimeVersion() && uuid.version() != 1) || uuid.version() != 4) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              String.format(
                  "Invalid UUID version. Expected version %d, but received version %d",
                  isTimeVersion() ? 1 : 4, uuid.version()))
          .asException();
    }
    return innerCodec.encode(uuid, ProtocolVersion.DEFAULT);
  }

  @NotNull
  @Override
  public QueryOuterClass.Value decode(@Nullable ByteBuffer bytes) throws StatusException {
    UUID uuid = innerCodec.decode(bytes, ProtocolVersion.DEFAULT);
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
