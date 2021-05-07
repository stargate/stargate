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

import static io.stargate.grpc.Values.*;
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
    "uuidValues",
    "listValues",
    "setValues",
    "mapValues",
    "tupleValues"
  })
  public void validValues(ColumnType type, Value expectedValue) {
    ValueCodec codec = ValueCodecs.get(type.rawType());
    assertThat(codec).isNotNull();
    ByteBuffer bytes = codec.encode(expectedValue, type);
    Value actualValue = codec.decode(bytes, type);
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
    "invalidUuidValues",
    "invalidListValues",
    "invalidSetValues",
    "invalidMapValues",
    "invalidTupleValues"
  })
  public void invalidValues(ColumnType type, Value value, String expectedMessage) {
    ValueCodec codec = ValueCodecs.get(type.rawType());
    assertThatThrownBy(() -> codec.encode(value, type))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(expectedMessage);
  }

  public static Stream<Arguments> bigintValues() {
    return Stream.of(
        arguments(Type.Bigint, intValue(0)),
        arguments(Type.Bigint, intValue(1)),
        arguments(Type.Bigint, intValue(Long.MAX_VALUE)),
        arguments(Type.Bigint, intValue(Long.MIN_VALUE)));
  }

  public static Stream<Arguments> invalidBigintValues() {
    return Stream.of(
        arguments(Type.Bigint, nullValue(), "Expected integer type"),
        arguments(Type.Bigint, unsetValue(), "Expected integer type"));
  }

  public static Stream<Arguments> booleanValues() {
    return Stream.of(
        arguments(Type.Boolean, booleanValue(true)), arguments(Type.Boolean, booleanValue(false)));
  }

  public static Stream<Arguments> invalidBooleanValues() {
    return Stream.of(
        arguments(Type.Boolean, nullValue(), "Expected boolean type"),
        arguments(Type.Boolean, unsetValue(), "Expected boolean type"));
  }

  public static Stream<Arguments> byteValues() {
    return Stream.of(
        arguments(Type.Blob, bytesValue(new byte[] {'a', 'b', 'c'})),
        arguments(Type.Blob, bytesValue(new byte[] {})));
  }

  public static Stream<Arguments> invalidByteValues() {
    return Stream.of(
        arguments(Type.Blob, nullValue(), "Expected bytes type"),
        arguments(Type.Blob, unsetValue(), "Expected bytes type"));
  }

  public static Stream<Arguments> dateValues() {
    return Stream.of(
        arguments(Type.Date, dateValue(LocalDate.of(2021, 2, 28))),
        arguments(Type.Date, dateValue(LocalDate.ofEpochDay(0))),
        arguments(Type.Date, dateValue(LocalDate.MAX)),
        arguments(Type.Date, dateValue(LocalDate.MIN)));
  }

  public static Stream<Arguments> invalidDateValues() {
    return Stream.of(
        arguments(Type.Date, nullValue(), "Expected date type"),
        arguments(Type.Date, unsetValue(), "Expected date type"));
  }

  public static Stream<Arguments> doubleValues() {
    return Stream.of(
        arguments(Type.Double, doubleValue(3.14159)),
        arguments(Type.Double, doubleValue(0)),
        arguments(Type.Double, doubleValue(Double.NaN)),
        arguments(Type.Double, doubleValue(Double.MAX_VALUE)),
        arguments(Type.Double, doubleValue(Double.MIN_VALUE)));
  }

  public static Stream<Arguments> invalidDoubleValues() {
    return Stream.of(
        arguments(Type.Double, nullValue(), "Expected double type"),
        arguments(Type.Double, unsetValue(), "Expected double type"));
  }

  public static Stream<Arguments> floatValues() {
    return Stream.of(
        arguments(Type.Float, floatValue(3.14159f)),
        arguments(Type.Float, floatValue(0)),
        arguments(Type.Float, floatValue(Float.NaN)),
        arguments(Type.Float, floatValue(Float.MAX_VALUE)),
        arguments(Type.Float, floatValue(Float.MIN_VALUE)));
  }

  public static Stream<Arguments> invalidFloatValues() {
    return Stream.of(
        arguments(Type.Float, nullValue(), "Expected float type"),
        arguments(Type.Float, unsetValue(), "Expected float type"));
  }

  public static Stream<Arguments> inetValues() throws UnknownHostException {
    return Stream.of(
        arguments(Type.Inet, inetValue(Inet4Address.getByName("127.0.0.1"))),
        arguments(Type.Inet, inetValue(Inet6Address.getByName("::0"))));
  }

  public static Stream<Arguments> invalidInetValues() {
    return Stream.of(
        arguments(Type.Inet, nullValue(), "Expected bytes type"),
        arguments(Type.Inet, unsetValue(), "Expected bytes type"),
        arguments(
            Type.Inet,
            bytesValue(new byte[] {}),
            "Expected 4 or 16 bytes for an IPv4 or IPv6 address"),
        arguments(
            Type.Inet,
            bytesValue(new byte[] {1, 2}),
            "Expected 4 or 16 bytes for an IPv4 or IPv6 address"),
        arguments(
            Type.Inet,
            bytesValue(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}),
            "Expected 4 or 16 bytes for an IPv4 or IPv6 address"));
  }

  public static Stream<Arguments> intValues() {
    return Stream.of(
        arguments(Type.Int, intValue(0)),
        arguments(Type.Int, intValue(1)),
        arguments(Type.Int, intValue(Integer.MAX_VALUE)),
        arguments(Type.Int, intValue(Integer.MIN_VALUE)));
  }

  public static Stream<Arguments> invalidIntValues() {
    return Stream.of(
        arguments(Type.Int, nullValue(), "Expected integer type"),
        arguments(Type.Int, unsetValue(), "Expected integer type"),
        arguments(
            Type.Int,
            intValue((long) Integer.MAX_VALUE + 1),
            "Valid range for int is -2147483648 to 2147483647"),
        arguments(
            Type.Int,
            intValue((long) Integer.MIN_VALUE - 1),
            "Valid range for int is -2147483648 to 2147483647"));
  }

  public static Stream<Arguments> smallintValues() {
    return Stream.of(
        arguments(Type.Smallint, intValue(0)),
        arguments(Type.Smallint, intValue(1)),
        arguments(Type.Smallint, intValue(Short.MAX_VALUE)),
        arguments(Type.Smallint, intValue(Short.MIN_VALUE)));
  }

  public static Stream<Arguments> invalidSmallintValues() {
    return Stream.of(
        arguments(Type.Smallint, nullValue(), "Expected integer type"),
        arguments(Type.Smallint, unsetValue(), "Expected integer type"),
        arguments(
            Type.Smallint,
            intValue((long) Short.MAX_VALUE + 1),
            "Valid range for smallint is -32768 to 32767"),
        arguments(
            Type.Smallint,
            intValue((long) Short.MIN_VALUE - 1),
            "Valid range for smallint is -32768 to 32767"));
  }

  public static Stream<Arguments> stringValues() {
    return Stream.of(
        arguments(Type.Ascii, stringValue("Hello, world")),
        arguments(Type.Varchar, stringValue("你好，世界")),
        arguments(Type.Text, stringValue("你好，世界")),
        arguments(Type.Varchar, stringValue("")),
        arguments(Type.Text, stringValue("")),
        arguments(Type.Ascii, stringValue("")));
  }

  public static Stream<Arguments> invalidStringValues() {
    return Stream.of(
        arguments(Type.Text, nullValue(), "Expected string type"),
        arguments(Type.Varchar, unsetValue(), "Expected string type"),
        arguments(
            Type.Ascii,
            stringValue("你好，世界"),
            "java.nio.charset.UnmappableCharacterException: Input length = 1"));
  }

  public static Stream<Arguments> tinyintValues() {
    return Stream.of(
        arguments(Type.Tinyint, intValue(0)),
        arguments(Type.Tinyint, intValue(1)),
        arguments(Type.Tinyint, intValue(Byte.MAX_VALUE)),
        arguments(Type.Tinyint, intValue(Byte.MIN_VALUE)));
  }

  public static Stream<Arguments> invalidTinyintValues() {
    return Stream.of(
        arguments(Type.Tinyint, nullValue(), "Expected integer type"),
        arguments(Type.Tinyint, unsetValue(), "Expected integer type"),
        arguments(
            Type.Tinyint,
            intValue((long) Byte.MAX_VALUE + 1),
            "Valid range for tinyint is -128 to 127"),
        arguments(
            Type.Tinyint,
            intValue((long) Byte.MIN_VALUE - 1),
            "Valid range for tinyint is -128 to 127"));
  }

  public static Stream<Arguments> timeValues() {
    return Stream.of(
        arguments(Type.Time, timeValue(LocalTime.now())),
        arguments(Type.Time, timeValue(LocalTime.MAX)),
        arguments(Type.Time, timeValue(LocalTime.MIN)));
  }

  public static Stream<Arguments> invalidTimeValues() {
    return Stream.of(
        arguments(Type.Time, nullValue(), "Expected time type"),
        arguments(Type.Time, unsetValue(), "Expected time type"),
        arguments(
            Type.Time,
            Value.newBuilder().setTime(-1).build(),
            "Valid range for time is 0 to 86399999999999 nanoseconds"));
  }

  public static Stream<Arguments> uuidValues() {
    return Stream.of(
        arguments(Type.Uuid, uuidValue(Uuids.random())),
        arguments(Type.Uuid, uuidValue(UUID.nameUUIDFromBytes("abc".getBytes()))),
        arguments(Type.Timeuuid, uuidValue(Uuids.timeBased())));
  }

  public static Stream<Arguments> invalidUuidValues() {
    return Stream.of(
        arguments(Type.Uuid, nullValue(), "Expected UUID type"),
        arguments(Type.Timeuuid, unsetValue(), "Expected UUID type"),
        arguments(
            Type.Timeuuid, uuidValue(UUID.randomUUID()), "is not a Type 1 (time-based) UUID"));
  }

  public static Stream<Arguments> listValues() {
    return Stream.of(
        arguments(Type.List.of(Type.Varchar), collection()),
        arguments(
            Type.List.of(Type.Varchar),
            collection(stringValue("a"), stringValue("b"), stringValue("c"))),
        arguments(Type.List.of(Type.Int), collection(intValue(1), intValue(2), intValue(3))));
  }

  public static Stream<Arguments> invalidListValues() {
    return Stream.of(
        arguments(
            Type.List.of(Type.Varchar),
            collection(stringValue("a"), intValue(1)),
            "Expected string type"),
        arguments(Type.List.of(Type.Varchar), collection(unsetValue()), "Expected string type"),
        arguments(Type.List.of(Type.Int), nullValue(), "Expected collection type"),
        arguments(Type.List.of(Type.Int), unsetValue(), "Expected collection type"),
        arguments(
            Type.List.of(Type.Varchar),
            collection(nullValue()),
            "null is not supported inside lists"),
        arguments(
            Type.List.of(Type.Int), collection(nullValue()), "null is not supported inside lists"));
  }

  public static Stream<Arguments> setValues() {
    return Stream.of(
        arguments(Type.Set.of(Type.Varchar), collection()),
        arguments(
            Type.Set.of(Type.Varchar),
            collection(stringValue("a"), stringValue("b"), stringValue("c"))));
  }

  public static Stream<Arguments> invalidSetValues() {
    return Stream.of(
        arguments(
            Type.Set.of(Type.Varchar),
            collection(nullValue()),
            "null is not supported inside sets"),
        arguments(
            Type.Set.of(Type.Int), collection(nullValue()), "null is not supported inside sets"));
  }

  public static Stream<Arguments> mapValues() {
    return Stream.of(
        arguments(Type.Map.of(Type.Varchar, Type.Int), collection()),
        arguments(
            Type.Map.of(Type.Varchar, Type.Int),
            collection(
                stringValue("a"), intValue(1),
                stringValue("b"), intValue(2),
                stringValue("c"), intValue(3))),
        arguments(
            Type.Map.of(Type.Uuid, Type.Varchar),
            collection(
                uuidValue(Uuids.random()), stringValue("a"),
                uuidValue(Uuids.random()), stringValue("b"),
                uuidValue(Uuids.random()), stringValue("c"))));
  }

  public static Stream<Arguments> invalidMapValues() {
    return Stream.of(
        arguments(
            Type.Map.of(Type.Varchar, Type.Int),
            collection(
                stringValue("a"), intValue(1),
                stringValue("b"), intValue(2),
                stringValue("c"), uuidValue(Uuids.random())),
            "Expected integer type"),
        arguments(
            Type.Map.of(Type.Varchar, Type.Int),
            collection(
                stringValue("a"), intValue(1),
                stringValue("b"), intValue(2),
                stringValue("c"), unsetValue()),
            "Expected integer type"),
        arguments(
            Type.Map.of(Type.Uuid, Type.Varchar),
            collection(
                uuidValue(Uuids.random()), stringValue("a"),
                uuidValue(Uuids.random()), stringValue("b"),
                intValue(1), stringValue("c")),
            "Expected UUID type"),
        arguments(
            Type.Map.of(Type.Uuid, Type.Varchar),
            collection(
                uuidValue(Uuids.random()), stringValue("a"),
                uuidValue(Uuids.random()), stringValue("b"),
                unsetValue(), stringValue("c")),
            "Expected UUID type"),
        arguments(
            Type.Map.of(Type.Uuid, Type.Varchar),
            collection(uuidValue(Uuids.random())),
            "Missing pair value (expected an even number of elements)"),
        arguments(Type.Map.of(Type.Uuid, Type.Varchar), nullValue(), "Expected collection type"),
        arguments(Type.Map.of(Type.Uuid, Type.Varchar), unsetValue(), "Expected collection type"),
        arguments(
            Type.Map.of(Type.Varchar, Type.Int),
            collection(stringValue("a"), nullValue()),
            "null is not supported inside maps"),
        arguments(
            Type.Map.of(Type.Uuid, Type.Varchar),
            collection(nullValue(), stringValue("a")),
            "null is not supported inside maps"));
  }

  public static Stream<Arguments> tupleValues() {
    return Stream.of(
        arguments(Type.Tuple.of(Type.Varchar, Type.Int, Type.Uuid), collection(stringValue("a"))),
        arguments(
            Type.Tuple.of(Type.Varchar, Type.Int, Type.Uuid),
            collection(stringValue("a"), intValue(1))),
        arguments(
            Type.Tuple.of(Type.Varchar, Type.Int, Type.Uuid),
            collection(stringValue("a"), intValue(1), uuidValue(Uuids.random()))),
        arguments(
            Type.Tuple.of(Type.Varchar, Type.Int, Type.Uuid),
            collection(nullValue(), nullValue(), nullValue())));
  }

  public static Stream<Arguments> invalidTupleValues() {
    return Stream.of(
        arguments(
            Type.Tuple.of(Type.Varchar, Type.Int, Type.Uuid),
            collection(stringValue("a"), intValue(1), stringValue("wrong")),
            "Expected UUID type"),
        arguments(
            Type.Tuple.of(Type.Varchar, Type.Int, Type.Uuid),
            collection(stringValue("a"), intValue(1), unsetValue()),
            "Expected UUID type"),
        arguments(
            Type.Tuple.of(Type.Varchar),
            collection(stringValue("a"), intValue(1)),
            "Too many tuple fields. Expected 1, but received 2"),
        arguments(
            Type.Tuple.of(Type.Varchar, Type.Int, Type.Uuid),
            nullValue(),
            "Expected collection type"),
        arguments(
            Type.Tuple.of(Type.Varchar, Type.Int, Type.Uuid),
            unsetValue(),
            "Expected collection type"));
  }
}
