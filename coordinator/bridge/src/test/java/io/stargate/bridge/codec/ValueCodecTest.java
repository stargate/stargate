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
package io.stargate.bridge.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.QueryOuterClass.Inet;
import io.stargate.bridge.proto.QueryOuterClass.Value;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.db.schema.Column.Type;
import io.stargate.db.schema.ImmutableUserDefinedType;
import io.stargate.db.schema.UserDefinedType;
import io.stargate.db.schema.VectorType;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.HashSet;
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
    "byteBufferValues",
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
    "tupleValues",
    "bigIntegerValues",
    "bigDecimalValues",
    "vectorValues",
  })
  public void validValues(ColumnType type, Value expectedValue) {
    ValueCodec codec = ValueCodecs.get(type.rawType());
    assertThat(codec).isNotNull();
    ByteBuffer bytes = codec.encode(expectedValue, type);
    Value actualValue = codec.decode(bytes, type);
    assertThat(actualValue).isEqualTo(expectedValue);

    // 27-Jan-2022, tatu: Specific kind of failure (see #1577) occurs if
    //    we try to decode second time from same ByteBuffer!
    Value actualValue2 = codec.decode(bytes, type);
    assertThat(actualValue2).isEqualTo(expectedValue);
  }

  @ParameterizedTest
  @MethodSource({"udtValues"})
  public void validValues(ColumnType type, Value value, Value expectedValue) {
    ValueCodec codec = ValueCodecs.get(type.rawType());
    assertThat(codec).isNotNull();
    ByteBuffer bytes = codec.encode(value, type);
    Value actualValue = codec.decode(bytes, type);
    assertThat(actualValue).isEqualTo(expectedValue);

    // 27-Jan-2022, tatu: Specific kind of failure (see #1577) occurs if
    //    we try to decode second time from same ByteBuffer!
    Value actualValue2 = codec.decode(bytes, type);
    assertThat(actualValue2).isEqualTo(expectedValue);
  }

  @ParameterizedTest
  @MethodSource({
    "invalidBigintValues",
    "invalidBooleanValues",
    "invalidByteValues",
    "invalidByteBufferValues",
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
    "invalidTupleValues",
    "invalidUdtValues",
    "invalidBigIntegerValues",
    "invalidBigDecimalValues",
    "invalidVectorValues",
  })
  public void invalidValues(ColumnType type, Value value, String expectedMessage) {
    ValueCodec codec = ValueCodecs.get(type.rawType());
    assertThatThrownBy(() -> codec.encode(value, type))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(expectedMessage);
  }

  public static Stream<Arguments> bigintValues() {
    return Stream.of(
        arguments(Type.Bigint, Values.of(0)),
        arguments(Type.Bigint, Values.of(1)),
        arguments(Type.Bigint, Values.of(Long.MAX_VALUE)),
        arguments(Type.Bigint, Values.of(Long.MIN_VALUE)));
  }

  public static Stream<Arguments> invalidBigintValues() {
    return Stream.of(
        arguments(Type.Bigint, Values.NULL, "Expected integer type"),
        arguments(Type.Bigint, Values.UNSET, "Expected integer type"));
  }

  public static Stream<Arguments> booleanValues() {
    return Stream.of(
        arguments(Type.Boolean, Values.of(true)), arguments(Type.Boolean, Values.of(false)));
  }

  public static Stream<Arguments> invalidBooleanValues() {
    return Stream.of(
        arguments(Type.Boolean, Values.NULL, "Expected boolean type"),
        arguments(Type.Boolean, Values.UNSET, "Expected boolean type"));
  }

  public static Stream<Arguments> byteValues() {
    return Stream.of(
        arguments(Type.Blob, Values.of(new byte[] {'a', 'b', 'c'})),
        arguments(Type.Blob, Values.of(new byte[] {})));
  }

  public static Stream<Arguments> byteBufferValues() {
    return Stream.of(
        arguments(Type.Blob, Values.of(ByteBuffer.wrap(new byte[] {'a', 'b', 'c'}))),
        arguments(Type.Blob, Values.of(ByteBuffer.wrap(new byte[] {}))));
  }

  public static Stream<Arguments> invalidByteValues() {
    return Stream.of(
        arguments(Type.Blob, Values.NULL, "Expected bytes type"),
        arguments(Type.Blob, Values.UNSET, "Expected bytes type"));
  }

  public static Stream<Arguments> invalidByteBufferValues() {
    return Stream.of(
        arguments(Type.Blob, Values.NULL, "Expected bytes type"),
        arguments(Type.Blob, Values.UNSET, "Expected bytes type"));
  }

  public static Stream<Arguments> dateValues() {
    return Stream.of(
        arguments(Type.Date, Values.of(LocalDate.of(2021, 2, 28))),
        arguments(Type.Date, Values.of(LocalDate.ofEpochDay(0))),
        arguments(Type.Date, Values.of(LocalDate.MAX)),
        arguments(Type.Date, Values.of(LocalDate.MIN)));
  }

  public static Stream<Arguments> invalidDateValues() {
    return Stream.of(
        arguments(Type.Date, Values.NULL, "Expected date type"),
        arguments(Type.Date, Values.UNSET, "Expected date type"));
  }

  public static Stream<Arguments> doubleValues() {
    return Stream.of(
        arguments(Type.Double, Values.of(3.14159)),
        arguments(Type.Double, Values.of(0d)),
        arguments(Type.Double, Values.of(Double.NaN)),
        arguments(Type.Double, Values.of(Double.MAX_VALUE)),
        arguments(Type.Double, Values.of(Double.MIN_VALUE)));
  }

  public static Stream<Arguments> invalidDoubleValues() {
    return Stream.of(
        arguments(Type.Double, Values.NULL, "Expected double type"),
        arguments(Type.Double, Values.UNSET, "Expected double type"));
  }

  public static Stream<Arguments> floatValues() {
    return Stream.of(
        arguments(Type.Float, Values.of(3.14159f)),
        arguments(Type.Float, Values.of(0f)),
        arguments(Type.Float, Values.of(Float.NaN)),
        arguments(Type.Float, Values.of(Float.MAX_VALUE)),
        arguments(Type.Float, Values.of(Float.MIN_VALUE)));
  }

  public static Stream<Arguments> invalidFloatValues() {
    return Stream.of(
        arguments(Type.Float, Values.NULL, "Expected float type"),
        arguments(Type.Float, Values.UNSET, "Expected float type"));
  }

  public static Stream<Arguments> inetValues() throws UnknownHostException {
    return Stream.of(
        arguments(Type.Inet, Values.of(Inet4Address.getByName("127.0.0.1"))),
        arguments(Type.Inet, Values.of(Inet6Address.getByName("::0"))));
  }

  public static Stream<Arguments> invalidInetValues() {
    return Stream.of(
        arguments(Type.Inet, Values.NULL, "Expected bytes type"),
        arguments(Type.Inet, Values.UNSET, "Expected bytes type"),
        arguments(
            Type.Inet,
            Value.newBuilder()
                .setInet(Inet.newBuilder().setValue(ByteString.copyFrom(new byte[] {})))
                .build(),
            "Expected 4 or 16 bytes for an IPv4 or IPv6 address"),
        arguments(
            Type.Inet,
            Value.newBuilder()
                .setInet(Inet.newBuilder().setValue(ByteString.copyFrom(new byte[] {1, 2})))
                .build(),
            "Expected 4 or 16 bytes for an IPv4 or IPv6 address"),
        arguments(
            Type.Inet,
            Value.newBuilder()
                .setInet(
                    Inet.newBuilder()
                        .setValue(
                            ByteString.copyFrom(
                                new byte[] {
                                  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
                                })))
                .build(),
            "Expected 4 or 16 bytes for an IPv4 or IPv6 address"));
  }

  public static Stream<Arguments> intValues() {
    return Stream.of(
        arguments(Type.Int, Values.of(0)),
        arguments(Type.Int, Values.of(1)),
        arguments(Type.Int, Values.of(Integer.MAX_VALUE)),
        arguments(Type.Int, Values.of(Integer.MIN_VALUE)));
  }

  public static Stream<Arguments> invalidIntValues() {
    return Stream.of(
        arguments(Type.Int, Values.NULL, "Expected integer type"),
        arguments(Type.Int, Values.UNSET, "Expected integer type"),
        arguments(
            Type.Int,
            Values.of((long) Integer.MAX_VALUE + 1),
            "Valid range for int is -2147483648 to 2147483647"),
        arguments(
            Type.Int,
            Values.of((long) Integer.MIN_VALUE - 1),
            "Valid range for int is -2147483648 to 2147483647"));
  }

  public static Stream<Arguments> smallintValues() {
    return Stream.of(
        arguments(Type.Smallint, Values.of(0)),
        arguments(Type.Smallint, Values.of(1)),
        arguments(Type.Smallint, Values.of(Short.MAX_VALUE)),
        arguments(Type.Smallint, Values.of(Short.MIN_VALUE)));
  }

  public static Stream<Arguments> invalidSmallintValues() {
    return Stream.of(
        arguments(Type.Smallint, Values.NULL, "Expected integer type"),
        arguments(Type.Smallint, Values.UNSET, "Expected integer type"),
        arguments(
            Type.Smallint,
            Values.of((long) Short.MAX_VALUE + 1),
            "Valid range for smallint is -32768 to 32767"),
        arguments(
            Type.Smallint,
            Values.of((long) Short.MIN_VALUE - 1),
            "Valid range for smallint is -32768 to 32767"));
  }

  public static Stream<Arguments> stringValues() {
    return Stream.of(
        arguments(Type.Ascii, Values.of("Hello, world")),
        arguments(Type.Text, Values.of("你好，世界")),
        arguments(Type.Text, Values.of("")),
        arguments(Type.Ascii, Values.of("")));
  }

  public static Stream<Arguments> invalidStringValues() {
    return Stream.of(
        arguments(Type.Text, Values.NULL, "Expected string type"),
        arguments(
            Type.Ascii,
            Values.of("你好，世界"),
            "java.nio.charset.UnmappableCharacterException: Input length = 1"));
  }

  public static Stream<Arguments> tinyintValues() {
    return Stream.of(
        arguments(Type.Tinyint, Values.of(0)),
        arguments(Type.Tinyint, Values.of(1)),
        arguments(Type.Tinyint, Values.of(Byte.MAX_VALUE)),
        arguments(Type.Tinyint, Values.of(Byte.MIN_VALUE)));
  }

  public static Stream<Arguments> invalidTinyintValues() {
    return Stream.of(
        arguments(Type.Tinyint, Values.NULL, "Expected integer type"),
        arguments(Type.Tinyint, Values.UNSET, "Expected integer type"),
        arguments(
            Type.Tinyint,
            Values.of((long) Byte.MAX_VALUE + 1),
            "Valid range for tinyint is -128 to 127"),
        arguments(
            Type.Tinyint,
            Values.of((long) Byte.MIN_VALUE - 1),
            "Valid range for tinyint is -128 to 127"));
  }

  public static Stream<Arguments> timeValues() {
    return Stream.of(
        arguments(Type.Time, Values.of(LocalTime.now())),
        arguments(Type.Time, Values.of(LocalTime.MAX)),
        arguments(Type.Time, Values.of(LocalTime.MIN)));
  }

  public static Stream<Arguments> invalidTimeValues() {
    return Stream.of(
        arguments(Type.Time, Values.NULL, "Expected time type"),
        arguments(Type.Time, Values.UNSET, "Expected time type"),
        arguments(
            Type.Time,
            Value.newBuilder().setTime(-1).build(),
            "Valid range for time is 0 to 86399999999999 nanoseconds"));
  }

  public static Stream<Arguments> uuidValues() {
    return Stream.of(
        arguments(Type.Uuid, Values.of(Uuids.random())),
        arguments(Type.Uuid, Values.of(UUID.nameUUIDFromBytes("abc".getBytes()))),
        arguments(Type.Timeuuid, Values.of(Uuids.timeBased())));
  }

  public static Stream<Arguments> invalidUuidValues() {
    final Value invalidEmptyUuid =
        Value.newBuilder()
            .setUuid(QueryOuterClass.Uuid.newBuilder().setValue(ByteString.copyFrom(new byte[0])))
            .build();
    final Value invalidLongUuid =
        Value.newBuilder()
            .setUuid(QueryOuterClass.Uuid.newBuilder().setValue(ByteString.copyFrom(new byte[24])))
            .build();

    return Stream.of(
        arguments(Type.Uuid, Values.NULL, "Expected UUID type"),
        arguments(Type.Timeuuid, Values.UNSET, "Expected UUID type"),
        arguments(Type.Uuid, invalidEmptyUuid, "Expected 16 bytes for a UUID, got 0"),
        arguments(Type.Timeuuid, invalidEmptyUuid, "Expected 16 bytes for a UUID, got 0"),
        arguments(Type.Uuid, invalidLongUuid, "Expected 16 bytes for a UUID, got 24"),
        arguments(Type.Timeuuid, invalidLongUuid, "Expected 16 bytes for a UUID, got 24"));
  }

  public static Stream<Arguments> listValues() {
    return Stream.of(
        arguments(Type.List.of(Type.Text), Values.of()),
        arguments(
            Type.List.of(Type.Text), Values.of(Values.of("a"), Values.of("b"), Values.of("c"))),
        arguments(Type.List.of(Type.Int), Values.of(Values.of(1), Values.of(2), Values.of(3))),
        arguments(
            Type.List.of(Type.Int),
            Values.of(Arrays.asList(Values.of(1), Values.of(2), Values.of(3)))));
  }

  public static Stream<Arguments> invalidListValues() {
    return Stream.of(
        arguments(
            Type.List.of(Type.Text),
            Values.of(Values.of("a"), Values.of(1)),
            "Expected string type"),
        arguments(Type.List.of(Type.Text), Values.of(Values.UNSET), "Expected string type"),
        arguments(Type.List.of(Type.Int), Values.NULL, "Expected collection type"),
        arguments(Type.List.of(Type.Int), Values.UNSET, "Expected collection type"),
        arguments(
            Type.List.of(Type.Text), Values.of(Values.NULL), "null is not supported inside lists"),
        arguments(
            Type.List.of(Type.Int), Values.of(Values.NULL), "null is not supported inside lists"));
  }

  public static Stream<Arguments> vectorValues() {
    Column.ColumnType vectorType = VectorType.of("org.apache.cassandra.db.marshal.FloatType", 4);
    return Stream.of(
        arguments(vectorType, Values.NULL),
        arguments(
            vectorType,
            Values.of(Values.of(1.0f), Values.of(1.1f), Values.of(1.2f), Values.of(1.3f))));
  }

  public static Stream<Arguments> invalidVectorValues() {
    Column.ColumnType vectorType = VectorType.of("org.apache.cassandra.db.marshal.FloatType", 10);
    return Stream.of(
        arguments(
            vectorType,
            Values.of(Values.of("1"), Values.NULL),
            "Expected collection of float type"),
        arguments(vectorType, Values.of(""), "Expected collection of float type"),
        arguments(vectorType, Values.UNSET, "Expected collection of float type"),
        arguments(vectorType, Values.of(Values.NULL), "Expected collection of float type"));
  }

  public static Stream<Arguments> setValues() {
    return Stream.of(
        arguments(Type.Set.of(Type.Text), Values.of()),
        arguments(
            Type.Set.of(Type.Text), Values.of(Values.of("a"), Values.of("b"), Values.of("c"))),
        arguments(
            Type.Set.of(Type.Text),
            Values.of(
                new HashSet<Value>() {
                  {
                    add(Values.of("a"));
                    add(Values.of("b"));
                    add(Values.of("c"));
                  }
                })));
  }

  public static Stream<Arguments> invalidSetValues() {
    return Stream.of(
        arguments(
            Type.Set.of(Type.Text), Values.of(Values.NULL), "null is not supported inside sets"),
        arguments(
            Type.Set.of(Type.Int), Values.of(Values.NULL), "null is not supported inside sets"));
  }

  public static Stream<Arguments> mapValues() {
    return Stream.of(
        arguments(Type.Map.of(Type.Text, Type.Int), Values.of()),
        arguments(
            Type.Map.of(Type.Text, Type.Int),
            Values.of(
                Values.of("a"), Values.of(1),
                Values.of("b"), Values.of(2),
                Values.of("c"), Values.of(3))),
        arguments(
            Type.Map.of(Type.Uuid, Type.Text),
            Values.of(
                Values.of(Uuids.random()), Values.of("a"),
                Values.of(Uuids.random()), Values.of("b"),
                Values.of(Uuids.random()), Values.of("c"))),
        arguments(
            Type.Map.of(Type.Uuid, Type.Text),
            Values.of(
                ImmutableMap.<Value, Value>builder()
                    .put(Values.of(Uuids.random()), Values.of("a"))
                    .put(Values.of(Uuids.random()), Values.of("b"))
                    .put(Values.of(Uuids.random()), Values.of("c"))
                    .build())));
  }

  public static Stream<Arguments> invalidMapValues() {
    return Stream.of(
        arguments(
            Type.Map.of(Type.Text, Type.Int),
            Values.of(
                Values.of("a"), Values.of(1),
                Values.of("b"), Values.of(2),
                Values.of("c"), Values.of(Uuids.random())),
            "Expected integer type"),
        arguments(
            Type.Map.of(Type.Text, Type.Int),
            Values.of(
                Values.of("a"), Values.of(1),
                Values.of("b"), Values.of(2),
                Values.of("c"), Values.UNSET),
            "Expected integer type"),
        arguments(
            Type.Map.of(Type.Uuid, Type.Text),
            Values.of(
                Values.of(Uuids.random()), Values.of("a"),
                Values.of(Uuids.random()), Values.of("b"),
                Values.of(1), Values.of("c")),
            "Expected UUID type"),
        arguments(
            Type.Map.of(Type.Uuid, Type.Text),
            Values.of(
                Values.of(Uuids.random()),
                Values.of("a"),
                Values.of(Uuids.random()),
                Values.of("b"),
                Values.UNSET,
                Values.of("c")),
            "Expected UUID type"),
        arguments(
            Type.Map.of(Type.Uuid, Type.Text),
            Values.of(Values.of(Uuids.random())),
            "Expected an even number of elements"),
        arguments(Type.Map.of(Type.Uuid, Type.Text), Values.NULL, "Expected collection type"),
        arguments(Type.Map.of(Type.Uuid, Type.Text), Values.UNSET, "Expected collection type"),
        arguments(
            Type.Map.of(Type.Text, Type.Int),
            Values.of(Values.of("a"), Values.NULL),
            "null is not supported inside maps"),
        arguments(
            Type.Map.of(Type.Uuid, Type.Text),
            Values.of(Values.NULL, Values.of("a")),
            "null is not supported inside maps"));
  }

  public static Stream<Arguments> tupleValues() {
    return Stream.of(
        arguments(Type.Tuple.of(Type.Text, Type.Int, Type.Uuid), Values.of(Values.of("a"))),
        arguments(
            Type.Tuple.of(Type.Text, Type.Int, Type.Uuid), Values.of(Values.of("a"), Values.of(1))),
        arguments(
            Type.Tuple.of(Type.Text, Type.Int, Type.Uuid),
            Values.of(Values.of("a"), Values.of(1), Values.of(Uuids.random()))),
        arguments(
            Type.Tuple.of(Type.Text, Type.Int, Type.Uuid),
            Values.of(Values.NULL, Values.NULL, Values.NULL)));
  }

  public static Stream<Arguments> invalidTupleValues() {
    return Stream.of(
        arguments(
            Type.Tuple.of(Type.Text, Type.Int, Type.Uuid),
            Values.of(Values.of("a"), Values.of(1), Values.of("wrong")),
            "Expected UUID type"),
        arguments(
            Type.Tuple.of(Type.Text, Type.Int, Type.Uuid),
            Values.of(Values.of("a"), Values.of(1), Values.UNSET),
            "Expected UUID type"),
        arguments(
            Type.Tuple.of(Type.Text),
            Values.of(Values.of("a"), Values.of(1)),
            "Too many tuple fields. Expected 1, but received 2"),
        arguments(
            Type.Tuple.of(Type.Text, Type.Int, Type.Uuid), Values.NULL, "Expected collection type"),
        arguments(
            Type.Tuple.of(Type.Text, Type.Int, Type.Uuid),
            Values.UNSET,
            "Expected collection type"));
  }

  public static Stream<Arguments> udtValues() {
    return Stream.of(
        arguments( // Simple case
            udt(Column.create("a", Type.Int), Column.create("b", Type.Text)),
            Values.udtOf(ImmutableMap.of("a", Values.of(1), "b", Values.of("abc"))),
            Values.udtOf(ImmutableMap.of("a", Values.of(1), "b", Values.of("abc")))),
        arguments( // Flipped
            udt(Column.create("a", Type.Int), Column.create("b", Type.Text)),
            Values.udtOf(ImmutableMap.of("a", Values.of(1), "b", Values.of("abc"))),
            Values.udtOf(ImmutableMap.of("b", Values.of("abc"), "a", Values.of(1)))),
        arguments( // Single first value
            udt(Column.create("a", Type.Int), Column.create("b", Type.Text)),
            Values.udtOf(ImmutableMap.of("a", Values.of(1))),
            Values.udtOf(ImmutableMap.of("a", Values.of(1), "b", Values.NULL))),
        arguments( // Single second value
            udt(Column.create("a", Type.Int), Column.create("b", Type.Text)),
            Values.udtOf(ImmutableMap.of("b", Values.of("abc"))),
            Values.udtOf(ImmutableMap.of("a", Values.NULL, "b", Values.of("abc")))),
        arguments( // Empty
            udt(Column.create("a", Type.Int), Column.create("b", Type.Text)),
            Values.udtOf(ImmutableMap.of()),
            Values.udtOf(ImmutableMap.of("a", Values.NULL, "b", Values.NULL))),
        arguments( // Null
            udt(Column.create("a", Type.Int), Column.create("b", Type.Text)),
            Values.udtOf(ImmutableMap.of("a", Values.NULL, "b", Values.NULL)),
            Values.udtOf(ImmutableMap.of("a", Values.NULL, "b", Values.NULL))),
        arguments( // Embedded UDT
            udt(
                Column.create(
                    "c", udt(Column.create("a", Type.Int), Column.create("b", Type.Text)))),
            Values.udtOf(
                ImmutableMap.of(
                    "c", Values.udtOf(ImmutableMap.of("a", Values.of(1), "b", Values.of("abc"))))),
            Values.udtOf(
                ImmutableMap.of(
                    "c", Values.udtOf(ImmutableMap.of("a", Values.of(1), "b", Values.of("abc")))))),
        arguments( // Embedded collections
            udt(
                Column.create("a", Type.List.of(Type.Int)),
                Column.create("b", Type.Tuple.of(Type.Text, Type.Boolean))),
            Values.udtOf(
                ImmutableMap.of(
                    "a",
                    Values.of(Values.of(1), Values.of(2), Values.of(3)),
                    "b",
                    Values.of(Values.of("c"), Values.of(true)))),
            Values.udtOf(
                ImmutableMap.of(
                    "a",
                    Values.of(Values.of(1), Values.of(2), Values.of(3)),
                    "b",
                    Values.of(Values.of("c"), Values.of(true))))));
  }

  public static Stream<Arguments> invalidUdtValues() {
    return Stream.of(
        arguments(
            udt(Column.create("a", Type.Int), Column.create("b", Type.Text)),
            Values.udtOf(ImmutableMap.of("c", Values.of(1))),
            "User-defined type doesn't contain a field named 'c'"),
        arguments(
            udt(Column.create("a", Type.Int), Column.create("b", Type.Text)),
            Values.udtOf(ImmutableMap.of("a", Values.of("abc"), "b", Values.of("abc"))),
            "Expected integer type"),
        arguments(
            udt(Column.create("a", Type.Int), Column.create("b", Type.Text)),
            Values.udtOf(ImmutableMap.of("a", Values.of(1), "b", Values.of(2))),
            "Expected string type"),
        arguments(
            udt(Column.create("a", Type.Int), Column.create("b", Type.Text)),
            Values.NULL,
            "Expected user-defined type"),
        arguments(
            udt(Column.create("a", Type.Int), Column.create("b", Type.Text)),
            Values.UNSET,
            "Expected user-defined type"));
  }

  private static UserDefinedType udt(Column... columns) {
    return ImmutableUserDefinedType.builder()
        .name("name") // Dummy value
        .keyspace("keyspace") // Dummy value
        .columns(Arrays.asList(columns))
        .build();
  }

  public static Stream<Arguments> bigIntegerValues() {
    return Stream.of(
        arguments(Type.Varint, Values.of(BigInteger.ZERO)),
        arguments(Type.Varint, Values.of(BigInteger.ONE)),
        arguments(Type.Varint, Values.of(BigInteger.valueOf(Long.MAX_VALUE))),
        arguments(Type.Varint, Values.of(BigInteger.valueOf(Long.MIN_VALUE))));
  }

  public static Stream<Arguments> invalidBigIntegerValues() {
    return Stream.of(
        arguments(Type.Varint, Values.NULL, "Expected varint type"),
        arguments(Type.Varint, Values.UNSET, "Expected varint type"));
  }

  public static Stream<Arguments> bigDecimalValues() {
    return Stream.of(
        arguments(Type.Decimal, Values.of(BigDecimal.ZERO)),
        arguments(Type.Decimal, Values.of(BigDecimal.ONE)),
        arguments(Type.Decimal, Values.of(BigDecimal.valueOf(Long.MAX_VALUE))),
        arguments(Type.Decimal, Values.of(BigDecimal.valueOf(Long.MIN_VALUE))));
  }

  public static Stream<Arguments> invalidBigDecimalValues() {
    return Stream.of(
        arguments(Type.Decimal, Values.NULL, "Expected decimal type"),
        arguments(Type.Decimal, Values.UNSET, "Expected decimal type"));
  }
}
