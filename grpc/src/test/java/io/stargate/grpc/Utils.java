package io.stargate.grpc;

import io.stargate.db.Result.Flag;
import io.stargate.db.Result.Prepared;
import io.stargate.db.Result.PreparedMetadata;
import io.stargate.db.Result.ResultMetadata;
import io.stargate.db.schema.Column;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.EnumSet;
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
}
