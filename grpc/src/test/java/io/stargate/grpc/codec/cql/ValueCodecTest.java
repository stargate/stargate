package io.stargate.grpc.codec.cql;

import static io.stargate.grpc.Utils.toValue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.google.protobuf.ByteString;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.db.schema.Column.Type;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.QueryOuterClass.Value.Null;
import io.stargate.proto.QueryOuterClass.Value.Unset;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ValueCodecTest {
  @ParameterizedTest
  @MethodSource({"validByteValues", "validIntValues", "validStringValues", "validUuidValues"})
  public void validValues(ColumnType type, Value expectedValue) {
    ValueCodec codec = ValueCodecs.CODECS.get(type);
    assertThat(codec).isNotNull();
    ByteBuffer bytes = codec.encode(expectedValue, type);
    Value actualValue = codec.decode(bytes);
    assertThat(actualValue).isEqualTo(expectedValue);
  }

  @ParameterizedTest
  @MethodSource({
    "invalidByteValues",
    "invalidIntValues",
    "invalidStringValues",
    "invalidUuidValues"
  })
  public void invalidValues(ColumnType type, Value value, String expectedMessage) {
    ValueCodec codec = ValueCodecs.CODECS.get(type);
    assertThat(codec).isNotNull();
    assertThatThrownBy(() -> codec.encode(value, type))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(expectedMessage);
  }

  public static Stream<Arguments> validByteValues() {
    return Stream.of(
        arguments(
            Type.Blob,
            Value.newBuilder().setBytes(ByteString.copyFrom(new byte[] {'a', 'b', 'c'})).build()),
        arguments(
            Type.Blob, Value.newBuilder().setBytes(ByteString.copyFrom(new byte[] {})).build()));
  }

  public static Stream<Arguments> invalidByteValues() {
    return Stream.of(
        arguments(Type.Blob, Value.newBuilder().setInt(0).build(), "Expected bytes type"),
        arguments(
            Type.Blob,
            Value.newBuilder().setNull(Null.newBuilder().build()).build(),
            "Expected bytes type"),
        arguments(
            Type.Blob,
            Value.newBuilder().setUnset(Unset.newBuilder().build()).build(),
            "Expected bytes type"));
  }

  public static Stream<Arguments> validIntValues() {
    return Stream.of(
        arguments(Type.Int, Value.newBuilder().setInt(0).build()),
        arguments(Type.Int, Value.newBuilder().setInt(1).build()),
        arguments(Type.Int, Value.newBuilder().setInt(Integer.MAX_VALUE).build()),
        arguments(Type.Int, Value.newBuilder().setInt(Integer.MIN_VALUE).build()));
  }

  public static Stream<Arguments> invalidIntValues() {
    return Stream.of(
        arguments(
            Type.Int,
            Value.newBuilder().setNull(Null.newBuilder().build()).build(),
            "Expected integer type"),
        arguments(
            Type.Int,
            Value.newBuilder().setUnset(Unset.newBuilder().build()).build(),
            "Expected integer type"),
        arguments(
            Type.Int,
            Value.newBuilder().setInt((long) Integer.MAX_VALUE + 1).build(),
            "Integer overflow"),
        arguments(
            Type.Int,
            Value.newBuilder().setInt((long) Integer.MIN_VALUE - 1).build(),
            "Integer overflow"));
  }

  public static Stream<Arguments> validStringValues() {
    return Stream.of(
        arguments(Type.Ascii, Value.newBuilder().setString("Hello, world").build()),
        arguments(Type.Varchar, Value.newBuilder().setString("你好，世界").build()),
        arguments(Type.Text, Value.newBuilder().setString("你好，世界").build()),
        arguments(Type.Varchar, Value.newBuilder().setString("").build()),
        arguments(Type.Text, Value.newBuilder().setString("").build()),
        arguments(Type.Ascii, Value.newBuilder().setString("").build()));
  }

  public static Stream<Arguments> invalidStringValues() {
    return Stream.of(
        arguments(
            Type.Text,
            Value.newBuilder().setNull(Null.newBuilder().build()).build(),
            "Expected string type"),
        arguments(
            Type.Varchar,
            Value.newBuilder().setUnset(Unset.newBuilder().build()).build(),
            "Expected string type"),
        arguments(
            Type.Ascii,
            Value.newBuilder().setString("你好，世界").build(),
            "java.nio.charset.UnmappableCharacterException: Input length = 1"));
  }

  public static Stream<Arguments> validUuidValues() {
    return Stream.of(
        arguments(Type.Uuid, toValue(Uuids.random())),
        arguments(Type.Uuid, toValue(UUID.nameUUIDFromBytes("abc".getBytes()))),
        arguments(Type.Timeuuid, toValue(Uuids.timeBased())));
  }

  public static Stream<Arguments> invalidUuidValues() {
    return Stream.of(
        arguments(
            Type.Uuid,
            Value.newBuilder().setNull(Null.newBuilder().build()).build(),
            "Expected UUID type"),
        arguments(
            Type.Timeuuid,
            Value.newBuilder().setUnset(Unset.newBuilder().build()).build(),
            "Expected UUID type"),
        arguments(Type.Timeuuid, toValue(UUID.randomUUID()), "is not a Type 1 (time-based) UUID"));
  }
}
