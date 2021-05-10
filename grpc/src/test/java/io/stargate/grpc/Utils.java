/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.grpc;

import com.google.protobuf.ByteString;
import io.stargate.db.Result.Flag;
import io.stargate.db.Result.Prepared;
import io.stargate.db.Result.PreparedMetadata;
import io.stargate.db.Result.ResultMetadata;
import io.stargate.db.schema.Column;
import io.stargate.proto.QueryOuterClass.Uuid;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.QueryOuterClass.Value.Null;
import io.stargate.proto.QueryOuterClass.Value.Unset;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.UUID;
import org.apache.cassandra.stargate.utils.MD5Digest;

public class Utils {
  public static final MD5Digest RESULT_METADATA_ID = MD5Digest.compute("resultMetadata");
  public static final MD5Digest STATEMENT_ID = MD5Digest.compute("statement");

  public static final EnumSet EMPTY_FLAGS = EnumSet.noneOf(Flag.class);

  public static ByteBuffer UNSET = ByteBuffer.allocate(0);

  private static final LocalDate EPOCH = LocalDate.of(1970, 1, 1);

  private static Value NULL_VALUE = Value.newBuilder().setNull(Null.newBuilder().build()).build();
  private static Value UNSET_VALUE =
      Value.newBuilder().setUnset(Unset.newBuilder().build()).build();

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

  public static Value nullValue() {
    return NULL_VALUE;
  }

  public static Value unsetValue() {
    return UNSET_VALUE;
  }

  public static Value booleanValue(boolean value) {
    return Value.newBuilder().setBoolean(value).build();
  }

  public static Value dateValue(LocalDate value) {
    long days = ChronoUnit.DAYS.between(EPOCH, value);
    int unsigned = (int) days - Integer.MIN_VALUE;
    return Value.newBuilder().setDate(unsigned).build();
  }

  public static Value floatValue(float value) {
    return Value.newBuilder().setFloat(value).build();
  }

  public static Value doubleValue(double value) {
    return Value.newBuilder().setDouble(value).build();
  }

  public static Value bytesValue(byte[] value) {
    return Value.newBuilder().setBytes(ByteString.copyFrom(value)).build();
  }

  public static Value inetValue(InetAddress value) {
    return bytesValue(value.getAddress());
  }

  public static Value intValue(long value) {
    return Value.newBuilder().setInt(value).build();
  }

  public static Value stringValue(String value) {
    return Value.newBuilder().setString(value).build();
  }

  public static Value timeValue(LocalTime value) {
    return Value.newBuilder().setTime(value.toNanoOfDay()).build();
  }

  public static Value uuidValue(UUID value) {
    return Value.newBuilder()
        .setUuid(
            Uuid.newBuilder()
                .setMsb(value.getMostSignificantBits())
                .setLsb(value.getLeastSignificantBits()))
        .build();
  }
}
