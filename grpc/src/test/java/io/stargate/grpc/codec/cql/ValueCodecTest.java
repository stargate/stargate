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
package io.stargate.grpc.codec.cql;

import static io.stargate.grpc.Utils.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.datastax.oss.driver.api.core.uuid.Uuids;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.db.schema.Column.Type;
import io.stargate.proto.QueryOuterClass.Value;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ValueCodecTest {

  @ParameterizedTest
  @MethodSource({
    "bigintValues",
    "booleanValues",
    "byteValues",
    "dateValues",
    "doubleValues",
    "floatValues",
    "inetValues",
    "intValues",
    "smallintValues",
    "stringValues",
    "timeValues",
    "tinyintValues",
    "uuidValues"
  })
  public void validValues(ColumnType type, Value expectedValue) {
    ValueCodec codec = ValueCodecs.CODECS.get(type);
    assertThat(codec).isNotNull();
    ByteBuffer bytes = codec.encode(expectedValue, type);
    Value actualValue = codec.decode(bytes);
    assertThat(actualValue).isEqualTo(expectedValue);
  }

  @ParameterizedTest
  @MethodSource({
    "invalidBigintValues",
    "invalidBooleanValues",
    "invalidByteValues",
    "invalidDateValues",
    "invalidDoubleValues",
    "invalidFloatValues",
    "invalidIntValues",
    "invalidInetValues",
    "invalidSmallintValues",
    "invalidStringValues",
    "invalidTimeValues",
    "invalidTinyintValues",
    "invalidUuidValues"
  })
  public void invalidValues(ColumnType type, Value value, String expectedMessage) {
    ValueCodec codec = ValueCodecs.CODECS.get(type);
    assertThat(codec).isNotNull();
    assertThatThrownBy(() -> codec.encode(value, type))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(expectedMessage);
  }

  public static Stream<Arguments> bigintValues() {
    return Stream.of(
        arguments(Type.Bigint, int_(0)),
        arguments(Type.Bigint, int_(1)),
        arguments(Type.Bigint, int_(Long.MAX_VALUE)),
        arguments(Type.Bigint, int_(Long.MIN_VALUE)));
  }

  public static Stream<Arguments> invalidBigintValues() {
    return Stream.of(
        arguments(Type.Bigint, null_(), "Expected integer type"),
        arguments(Type.Bigint, unset(), "Expected integer type"));
  }

  public static Stream<Arguments> booleanValues() {
    return Stream.of(
        arguments(Type.Boolean, boolean_(true)), arguments(Type.Boolean, boolean_(false)));
  }

  public static Stream<Arguments> invalidBooleanValues() {
    return Stream.of(
        arguments(Type.Boolean, null_(), "Expected boolean type"),
        arguments(Type.Boolean, unset(), "Expected boolean type"));
  }

  public static Stream<Arguments> byteValues() {
    return Stream.of(
        arguments(Type.Blob, bytes(new byte[] {'a', 'b', 'c'})),
        arguments(Type.Blob, bytes(new byte[] {})));
  }

  public static Stream<Arguments> invalidByteValues() {
    return Stream.of(
        arguments(Type.Blob, null_(), "Expected bytes type"),
        arguments(Type.Blob, unset(), "Expected bytes type"));
  }

  public static Stream<Arguments> dateValues() {
    return Stream.of(
        arguments(Type.Date, date(LocalDate.of(2021, 2, 28))),
        arguments(Type.Date, date(LocalDate.ofEpochDay(0))),
        arguments(Type.Date, date(LocalDate.MAX)),
        arguments(Type.Date, date(LocalDate.MIN)));
  }

  public static Stream<Arguments> invalidDateValues() {
    return Stream.of(
        arguments(Type.Date, null_(), "Expected date type"),
        arguments(Type.Date, unset(), "Expected date type"));
  }

  public static Stream<Arguments> doubleValues() {
    return Stream.of(
        arguments(Type.Double, double_(3.14159)),
        arguments(Type.Double, double_(0)),
        arguments(Type.Double, double_(Double.NaN)),
        arguments(Type.Double, double_(Double.MAX_VALUE)),
        arguments(Type.Double, double_(Double.MIN_VALUE)));
  }

  public static Stream<Arguments> invalidDoubleValues() {
    return Stream.of(
        arguments(Type.Double, null_(), "Expected double type"),
        arguments(Type.Double, unset(), "Expected double type"));
  }

  public static Stream<Arguments> floatValues() {
    return Stream.of(
        arguments(Type.Float, float_(3.14159f)),
        arguments(Type.Float, float_(0)),
        arguments(Type.Float, float_(Float.NaN)),
        arguments(Type.Float, float_(Float.MAX_VALUE)),
        arguments(Type.Float, float_(Float.MIN_VALUE)));
  }

  public static Stream<Arguments> invalidFloatValues() {
    return Stream.of(
        arguments(Type.Float, null_(), "Expected float type"),
        arguments(Type.Float, unset(), "Expected float type"));
  }

  public static Stream<Arguments> inetValues() throws UnknownHostException {
    return Stream.of(
        arguments(Type.Inet, inet(Inet4Address.getByName("127.0.0.1"))),
        arguments(Type.Inet, inet(Inet6Address.getByName("::0"))));
  }

  public static Stream<Arguments> invalidInetValues() {
    return Stream.of(
        arguments(Type.Inet, null_(), "Expected bytes type"),
        arguments(Type.Inet, unset(), "Expected bytes type"),
        arguments(
            Type.Inet, bytes(new byte[] {}), "Expected 4 or 16 bytes for an IPv4 or IPv6 address"),
        arguments(
            Type.Inet,
            bytes(new byte[] {1, 2}),
            "Expected 4 or 16 bytes for an IPv4 or IPv6 address"),
        arguments(
            Type.Inet,
            bytes(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}),
            "Expected 4 or 16 bytes for an IPv4 or IPv6 address"));
  }

  public static Stream<Arguments> intValues() {
    return Stream.of(
        arguments(Type.Int, int_(0)),
        arguments(Type.Int, int_(1)),
        arguments(Type.Int, int_(Integer.MAX_VALUE)),
        arguments(Type.Int, int_(Integer.MIN_VALUE)));
  }

  public static Stream<Arguments> invalidIntValues() {
    return Stream.of(
        arguments(Type.Int, null_(), "Expected integer type"),
        arguments(Type.Int, unset(), "Expected integer type"),
        arguments(
            Type.Int,
            int_((long) Integer.MAX_VALUE + 1),
            "Valid range for int is -2147483648 to 2147483647"),
        arguments(
            Type.Int,
            int_((long) Integer.MIN_VALUE - 1),
            "Valid range for int is -2147483648 to 2147483647"));
  }

  public static Stream<Arguments> smallintValues() {
    return Stream.of(
        arguments(Type.Smallint, int_(0)),
        arguments(Type.Smallint, int_(1)),
        arguments(Type.Smallint, int_(Short.MAX_VALUE)),
        arguments(Type.Smallint, int_(Short.MIN_VALUE)));
  }

  public static Stream<Arguments> invalidSmallintValues() {
    return Stream.of(
        arguments(Type.Smallint, null_(), "Expected integer type"),
        arguments(Type.Smallint, unset(), "Expected integer type"),
        arguments(
            Type.Smallint,
            int_((long) Short.MAX_VALUE + 1),
            "Valid range for smallint is -32768 to 32767"),
        arguments(
            Type.Smallint,
            int_((long) Short.MIN_VALUE - 1),
            "Valid range for smallint is -32768 to 32767"));
  }

  public static Stream<Arguments> stringValues() {
    return Stream.of(
        arguments(Type.Ascii, string("Hello, world")),
        arguments(Type.Varchar, string("你好，世界")),
        arguments(Type.Text, string("你好，世界")),
        arguments(Type.Varchar, string("")),
        arguments(Type.Text, string("")),
        arguments(Type.Ascii, string("")));
  }

  public static Stream<Arguments> invalidStringValues() {
    return Stream.of(
        arguments(Type.Text, null_(), "Expected string type"),
        arguments(Type.Varchar, unset(), "Expected string type"),
        arguments(
            Type.Ascii,
            string("你好，世界"),
            "java.nio.charset.UnmappableCharacterException: Input length = 1"));
  }

  public static Stream<Arguments> tinyintValues() {
    return Stream.of(
        arguments(Type.Tinyint, int_(0)),
        arguments(Type.Tinyint, int_(1)),
        arguments(Type.Tinyint, int_(Byte.MAX_VALUE)),
        arguments(Type.Tinyint, int_(Byte.MIN_VALUE)));
  }

  public static Stream<Arguments> invalidTinyintValues() {
    return Stream.of(
        arguments(Type.Tinyint, null_(), "Expected integer type"),
        arguments(Type.Tinyint, unset(), "Expected integer type"),
        arguments(
            Type.Tinyint,
            int_((long) Byte.MAX_VALUE + 1),
            "Valid range for tinyint is -128 to 127"),
        arguments(
            Type.Tinyint,
            int_((long) Byte.MIN_VALUE - 1),
            "Valid range for tinyint is -128 to 127"));
  }

  public static Stream<Arguments> timeValues() {
    return Stream.of(
        arguments(Type.Time, time(LocalTime.now())),
        arguments(Type.Time, time(LocalTime.MAX)),
        arguments(Type.Time, time(LocalTime.MIN)));
  }

  public static Stream<Arguments> invalidTimeValues() {
    return Stream.of(
        arguments(Type.Time, null_(), "Expected time type"),
        arguments(Type.Time, unset(), "Expected time type"),
        arguments(
            Type.Time,
            Value.newBuilder().setTime(-1).build(),
            "Valid range for time is 0 to 86399999999999 nanoseconds"));
  }

  public static Stream<Arguments> uuidValues() {
    return Stream.of(
        arguments(Type.Uuid, uuid(Uuids.random())),
        arguments(Type.Uuid, uuid(UUID.nameUUIDFromBytes("abc".getBytes()))),
        arguments(Type.Timeuuid, uuid(Uuids.timeBased())));
  }

  public static Stream<Arguments> invalidUuidValues() {
    return Stream.of(
        arguments(Type.Uuid, null_(), "Expected UUID type"),
        arguments(Type.Timeuuid, unset(), "Expected UUID type"),
        arguments(Type.Timeuuid, uuid(UUID.randomUUID()), "is not a Type 1 (time-based) UUID"));
  }
}
