package io.stargate.grpc;

import io.stargate.db.Result.Flag;
import io.stargate.db.Result.Prepared;
import io.stargate.db.Result.PreparedMetadata;
import io.stargate.db.Result.ResultMetadata;
import io.stargate.db.schema.Column;
import io.stargate.proto.QueryOuterClass.Uuid;
import io.stargate.proto.QueryOuterClass.Value;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.UUID;
import org.apache.cassandra.stargate.utils.MD5Digest;

public class Utils {
  public static final MD5Digest RESULT_METADATA_ID = MD5Digest.compute("resultMetadata");
  public static final MD5Digest STATEMENT_ID = MD5Digest.compute("statement");

  public static final EnumSet EMPTY_FLAGS = EnumSet.noneOf(Flag.class);

  public static ByteBuffer UNSET = ByteBuffer.allocate(0);

  public static ResultMetadata makeResultMetadata(Column... columns) {
    return new ResultMetadata(
        EMPTY_FLAGS, columns.length, Arrays.asList(columns), RESULT_METADATA_ID, null);
  }

  public static PreparedMetadata makePreparedMetadata(Column... columns) {
    return new PreparedMetadata(EMPTY_FLAGS, Arrays.asList(columns), null);
  }

  public static Prepared makePrepared(Column... bindColumns) {
    return new Prepared(
        Utils.STATEMENT_ID,
        Utils.RESULT_METADATA_ID,
        Utils.makeResultMetadata(),
        Utils.makePreparedMetadata(bindColumns));
  }

  public static Value toValue(UUID value) {
    return Value.newBuilder()
        .setUuid(
            Uuid.newBuilder()
                .setMsb(value.getMostSignificantBits())
                .setLsb(value.getLeastSignificantBits()))
        .build();
  }

  public static Value toValue(long value) {
    return Value.newBuilder().setInt(value).build();
  }

  public static Value toValue(String value) {
    return Value.newBuilder().setString(value).build();
  }
}
