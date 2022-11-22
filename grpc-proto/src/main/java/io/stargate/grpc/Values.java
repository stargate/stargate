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
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.QueryOuterClass.Collection;
import io.stargate.proto.QueryOuterClass.Inet;
import io.stargate.proto.QueryOuterClass.UdtValue;
import io.stargate.proto.QueryOuterClass.Uuid;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.QueryOuterClass.Value.InnerCase;
import io.stargate.proto.QueryOuterClass.Value.Null;
import io.stargate.proto.QueryOuterClass.Value.Unset;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Values {

  public static Value NULL = Value.newBuilder().setNull(Null.newBuilder().build()).build();
  public static Value UNSET = Value.newBuilder().setUnset(Unset.newBuilder().build()).build();

  private static final LocalDate EPOCH = LocalDate.of(1970, 1, 1);

  public static Value of(boolean value) {
    return Value.newBuilder().setBoolean(value).build();
  }

  public static Value of(LocalDate value) {
    long days = ChronoUnit.DAYS.between(EPOCH, value);
    int unsigned = (int) days - Integer.MIN_VALUE;
    return Value.newBuilder().setDate(unsigned).build();
  }

  public static Value of(float value) {
    return Value.newBuilder().setFloat(value).build();
  }

  public static Value of(short value) {
    return Value.newBuilder().setInt(value).build();
  }

  public static Value of(byte value) {
    return Value.newBuilder().setInt(value).build();
  }

  public static Value of(double value) {
    return Value.newBuilder().setDouble(value).build();
  }

  public static Value of(byte[] value) {
    return Value.newBuilder().setBytes(ByteString.copyFrom(value)).build();
  }

  /**
   * Note that this method "consumes" its argument: after the call, {@code value} will have its
   * position set to its limit, and thus appear empty.
   */
  public static Value of(ByteBuffer value) {
    return Value.newBuilder().setBytes(ByteString.copyFrom(value)).build();
  }

  public static Value of(InetAddress value) {
    return Value.newBuilder()
        .setInet(Inet.newBuilder().setValue(ByteString.copyFrom(value.getAddress())))
        .build();
  }

  public static Value of(long value) {
    return Value.newBuilder().setInt(value).build();
  }

  public static Value of(String value) {
    return Value.newBuilder().setString(value).build();
  }

  public static Value of(LocalTime value) {
    return Value.newBuilder().setTime(value.toNanoOfDay()).build();
  }

  public static Value of(BigInteger value) {
    return Value.newBuilder()
        .setVarint(
            QueryOuterClass.Varint.newBuilder()
                .setValue(ByteString.copyFrom(value.toByteArray()))
                .build())
        .build();
  }

  public static Value of(BigDecimal value) {
    return Value.newBuilder()
        .setDecimal(
            QueryOuterClass.Decimal.newBuilder()
                .setValue(ByteString.copyFrom(value.unscaledValue().toByteArray()))
                .setScale(value.scale())
                .build())
        .build();
  }

  public static Value of(UUID value) {
    // The encoding format of our UUID is 16-bytes in big-endian byte order. Note: The initial byte
    // order of `ByteBuffer` is always big-endian.
    ByteBuffer bytes = ByteBuffer.allocate(16);
    bytes.putLong(0, value.getMostSignificantBits());
    bytes.putLong(8, value.getLeastSignificantBits());
    return Value.newBuilder()
        .setUuid(Uuid.newBuilder().setValue(ByteString.copyFrom(bytes)))
        .build();
  }

  public static Value of(Value... elements) {
    return Value.newBuilder()
        .setCollection(Collection.newBuilder().addAllElements(Arrays.asList(elements)).build())
        .build();
  }

  public static Value of(List<Value> elements) {
    return Value.newBuilder()
        .setCollection(Collection.newBuilder().addAllElements(elements).build())
        .build();
  }

  public static Value of(Set<Value> elements) {
    return Value.newBuilder()
        .setCollection(Collection.newBuilder().addAllElements(elements).build())
        .build();
  }

  public static Value udtOf(Map<String, Value> fields) {
    return Value.newBuilder().setUdt(UdtValue.newBuilder().putAllFields(fields).build()).build();
  }

  public static Value of(Map<Value, Value> fields) {

    // map is stored as a list of altering k,v
    // For example, for a Map([1,"a"], [2,"b"]) it becomes a List(1,"a",2,"b")
    List<Value> values =
        fields.entrySet().stream()
            .flatMap(v -> Stream.of(v.getKey(), v.getValue()))
            .collect(Collectors.toList());

    return Value.newBuilder()
        .setCollection(Collection.newBuilder().addAllElements(values).build())
        .build();
  }

  public static boolean bool(Value value) {
    checkInnerCase(value, InnerCase.BOOLEAN);

    return value.getBoolean();
  }

  public static int int_(Value value) {
    checkInnerCase(value, InnerCase.INT);

    int intValue = (int) value.getInt();
    if (intValue != value.getInt()) {
      throw new IllegalArgumentException(
          String.format(
              "Valid range for int is %d to %d, got %d",
              Integer.MIN_VALUE, Integer.MAX_VALUE, value.getInt()));
    }
    return intValue;
  }

  public static long bigint(Value value) {
    checkInnerCase(value, InnerCase.INT);

    return value.getInt();
  }

  public static short smallint(Value value) {
    checkInnerCase(value, InnerCase.INT);

    short shortValue = (short) value.getInt();
    if (shortValue != value.getInt()) {
      throw new IllegalArgumentException(
          String.format(
              "Valid range for smallint is %d to %d, got %d",
              Short.MIN_VALUE, Short.MAX_VALUE, value.getInt()));
    }
    return shortValue;
  }

  public static byte tinyint(Value value) {
    checkInnerCase(value, InnerCase.INT);

    byte byteValue = (byte) value.getInt();
    if (byteValue != value.getInt()) {
      throw new IllegalArgumentException(
          String.format(
              "Valid range for tinyint is %d to %d, got %d",
              Byte.MIN_VALUE, Byte.MAX_VALUE, value.getInt()));
    }
    return byteValue;
  }

  public static float float_(Value value) {
    checkInnerCase(value, InnerCase.FLOAT);

    return value.getFloat();
  }

  public static double double_(Value value) {
    checkInnerCase(value, InnerCase.DOUBLE);

    return value.getDouble();
  }

  public static ByteBuffer byteBuffer(Value value) {
    checkInnerCase(value, InnerCase.BYTES);

    return value.getBytes().asReadOnlyByteBuffer();
  }

  public static byte[] bytes(Value value) {
    checkInnerCase(value, InnerCase.BYTES);

    return value.getBytes().toByteArray();
  }

  public static String string(Value value) {
    checkInnerCase(value, InnerCase.STRING);

    return value.getString();
  }

  public static UUID uuid(Value value) {
    checkInnerCase(value, InnerCase.UUID);
    ByteString inBytes = value.getUuid().getValue();

    if (inBytes.size() != 16) {
      throw new IllegalArgumentException(
          "Expected 16 bytes for a uuid values, got " + inBytes.size());
    }
    ByteBuffer outBytes = ByteBuffer.allocate(16);
    inBytes.copyTo(outBytes);
    return new UUID(outBytes.getLong(0), outBytes.getLong(8));
  }

  public static InetAddress inet(Value value) {
    checkInnerCase(value, InnerCase.INET);

    int size = value.getInet().getValue().size();
    if (size != 4 && size != 16) {
      throw new IllegalArgumentException(
          "Expected 4 bytes (IPv4) or 16 (IPv6) bytes for a inet values, got " + size);
    }
    try {
      return InetAddress.getByAddress(value.getInet().getValue().toByteArray());
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException("Invalid bytes in inet value");
    }
  }

  public static BigInteger varint(Value value) {
    checkInnerCase(value, InnerCase.VARINT);

    return new BigInteger(value.getVarint().getValue().toByteArray());
  }

  public static BigDecimal decimal(Value value) {
    checkInnerCase(value, InnerCase.DECIMAL);

    return new BigDecimal(
        new BigInteger(value.getDecimal().getValue().toByteArray()), value.getDecimal().getScale());
  }

  public static LocalDate date(Value value) {
    checkInnerCase(value, InnerCase.DATE);

    int unsigned = value.getDate();
    int signed = unsigned + Integer.MIN_VALUE;
    return EPOCH.plusDays(signed);
  }

  public static LocalTime time(Value value) {
    checkInnerCase(value, InnerCase.TIME);

    return LocalTime.ofNanoOfDay(value.getTime());
  }

  private static void checkInnerCase(Value value, InnerCase expected) {
    if (value.getInnerCase() != expected) {
      throw new IllegalArgumentException(
          String.format("Expected %s value, received %s", expected, value.getInnerCase()));
    }
  }
}
