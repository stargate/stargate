package io.stargate.grpc.codec.cql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.google.protobuf.ByteString;
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

public class BytesCodecTest {
  private static final Column.Type TYPE = Type.Blob;

  private static ValueCodec codec;

  @BeforeAll
  public static void setup() {
    codec = ValueCodecs.CODECS.get(TYPE);
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
  public void invalidValues(Value value) {
    assertThatThrownBy(() -> codec.encode(value, TYPE))
        .isInstanceOf(StatusException.class)
        .hasMessageContaining("Expected bytes type");
  }

  public static Stream<Arguments> getValidValues() {
    return Stream.of(
        arguments(
            Value.newBuilder().setBytes(ByteString.copyFrom(new byte[] {'a', 'b', 'c'})).build()),
        arguments(Value.newBuilder().setBytes(ByteString.copyFrom(new byte[] {})).build()));
  }

  public static Stream<Arguments> getInvalidValues() {
    return Stream.of(
        arguments(Value.newBuilder().setInt(0).build()),
        arguments(Value.newBuilder().setNull(Null.newBuilder().build()).build()),
        arguments(Value.newBuilder().setUnset(Unset.newBuilder().build()).build()));
  }
}
