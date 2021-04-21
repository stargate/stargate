package io.stargate.grpc.codec.cql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import io.grpc.StatusException;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.Type;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.QueryOuterClass.Value.Null;
import io.stargate.proto.QueryOuterClass.Value.Unset;
import java.nio.ByteBuffer;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class IntCodecTest {
  private static final Column.Type TYPE = Type.Int;

  private static ValueCodec codec;

  @BeforeAll
  public static void setup() {
    codec = ValueCodecs.CODECS.get(Type.Int);
    assertThat(codec).isNotNull();
  }

  @ParameterizedTest
  @MethodSource("getValidValues")
  public void validValues(Value expectedValue) throws StatusException {
    ByteBuffer bytes = codec.encode(expectedValue, TYPE);
    Value actualValue = codec.decode(bytes);
    assertThat(actualValue).isEqualTo(expectedValue);
  }

  @ParameterizedTest
  @MethodSource("getInvalidValues")
  public void invalidValues(Value value, String expectedMessage) {
    assertThatThrownBy(() -> codec.encode(value, TYPE))
        .isInstanceOf(StatusException.class)
        .hasMessageContaining(expectedMessage);
  }

  public static Stream<Arguments> getValidValues() {
    return Stream.of(
        arguments(Value.newBuilder().setInt(0).build()),
        arguments(Value.newBuilder().setInt(1).build()),
        arguments(Value.newBuilder().setInt(Integer.MAX_VALUE).build()),
        arguments(Value.newBuilder().setInt(Integer.MIN_VALUE).build()));
  }

  public static Stream<Arguments> getInvalidValues() {
    return Stream.of(
        arguments(
            Value.newBuilder().setNull(Null.newBuilder().build()).build(), "Expected integer type"),
        arguments(
            Value.newBuilder().setUnset(Unset.newBuilder().build()).build(),
            "Expected integer type"),
        arguments(
            Value.newBuilder().setInt((long) Integer.MAX_VALUE + 1).build(), "Integer overflow"),
        arguments(
            Value.newBuilder().setInt((long) Integer.MIN_VALUE - 1).build(), "Integer overflow"));
  }
}
