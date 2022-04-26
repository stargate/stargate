package io.stargate.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.ByteString;
import io.stargate.proto.QueryOuterClass.Inet;
import io.stargate.proto.QueryOuterClass.Uuid;
import io.stargate.proto.QueryOuterClass.Value;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class ValuesTest {

  @Nested
  class BoolTest {

    @Test
    public void encodeDecode() {
      boolean expected = true;
      assertThat(Values.bool(Values.of(expected))).isEqualTo(expected);
    }

    @Test
    public void invalidType() {
      assertThatThrownBy(
              () -> {
                Values.bool(Values.NULL);
              })
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Expected BOOLEAN value, received NULL");
    }
  }

  @Nested
  class IntTest {

    @Test
    public void encodeDecode() {
      int expected = Integer.MAX_VALUE;
      assertThat(Values.int_(Values.of(expected))).isEqualTo(expected);
    }

    @Test
    public void invalidType() {
      assertThatThrownBy(
              () -> {
                Values.int_(Values.NULL);
              })
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Expected INT value, received NULL");
    }

    @Test
    public void invalidRange() {
      assertThatThrownBy(
              () -> {
                Values.int_(Values.of((long) Integer.MAX_VALUE + 1));
              })
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Valid range for int");

      assertThatThrownBy(
              () -> {
                Values.int_(Values.of((long) Integer.MIN_VALUE - 1));
              })
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Valid range for int");
    }
  }

  @Nested
  class BigintTest {

    @Test
    public void encodeDecode() {
      long expected = Long.MAX_VALUE;
      assertThat(Values.bigint(Values.of(expected))).isEqualTo(expected);
    }

    @Test
    public void invalidType() {
      assertThatThrownBy(
              () -> {
                Values.bigint(Values.NULL);
              })
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Expected INT value, received NULL");
    }
  }

  @Nested
  class SmallintTest {

    @Test
    public void encodeDecode() {
      short expected = Short.MAX_VALUE;
      assertThat(Values.smallint(Values.of(expected))).isEqualTo(expected);
    }

    @Test
    public void invalidType() {
      assertThatThrownBy(
              () -> {
                Values.smallint(Values.NULL);
              })
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Expected INT value, received NULL");
    }

    @Test
    public void invalidRange() {
      assertThatThrownBy(
              () -> {
                Values.smallint(Values.of((int) Short.MAX_VALUE + 1));
              })
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Valid range for smallint");

      assertThatThrownBy(
              () -> {
                Values.smallint(Values.of((int) Short.MIN_VALUE - 1));
              })
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Valid range for smallint");
    }
  }

  @Nested
  class TinyintTest {

    @Test
    public void encodeDecode() {
      byte expected = Byte.MAX_VALUE;
      assertThat(Values.tinyint(Values.of(expected))).isEqualTo(expected);
    }

    @Test
    public void invalidType() {
      assertThatThrownBy(
              () -> {
                Values.tinyint(Values.NULL);
              })
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Expected INT value, received NULL");
    }

    @Test
    public void invalidRange() {
      assertThatThrownBy(
              () -> {
                Values.tinyint(Values.of((short) Byte.MAX_VALUE + 1));
              })
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Valid range for tinyint");

      assertThatThrownBy(
              () -> {
                Values.tinyint(Values.of((short) Byte.MIN_VALUE - 1));
              })
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Valid range for tinyint");
    }
  }

  @Nested
  class FloatTest {

    @Test
    public void encodeDecode() {
      float expected = Float.MAX_VALUE;
      assertThat(Values.float_(Values.of(expected))).isEqualTo(expected);
    }

    @Test
    public void invalidType() {
      assertThatThrownBy(
              () -> {
                Values.float_(Values.NULL);
              })
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Expected FLOAT value, received NULL");
    }
  }

  @Nested
  class DoubleTest {

    @Test
    public void encodeDecode() {
      double expected = Double.MAX_VALUE;
      assertThat(Values.double_(Values.of(expected))).isEqualTo(expected);
    }

    @Test
    public void invalidType() {
      assertThatThrownBy(
              () -> {
                Values.double_(Values.NULL);
              })
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Expected DOUBLE value, received NULL");
    }
  }

  @Nested
  class BytesTest {

    @Test
    public void encodeDecodeBytes() {
      byte[] expected = new byte[] {1, 2, 3};
      assertThat(Values.bytes(Values.of(expected))).isEqualTo(expected);
    }

    @Test
    public void encodeDecodeByteBuffer() {
      ByteBuffer expected = ByteBuffer.wrap(new byte[] {1, 2, 3});
      assertThat(Values.byteBuffer(Values.of(expected.duplicate()))).isEqualTo(expected);
    }

    @Test
    public void invalidBytes() {
      assertThatThrownBy(
              () -> {
                Values.bytes(Values.NULL);
              })
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Expected BYTES value, received NULL");
    }

    @Test
    public void invalidByteBuffer() {
      assertThatThrownBy(
              () -> {
                Values.byteBuffer(Values.NULL);
              })
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Expected BYTES value, received NULL");
    }
  }

  @Nested
  class StringTest {

    @Test
    public void encodeDecode() {
      String expected = "abc";
      assertThat(Values.string(Values.of(expected))).isEqualTo(expected);
    }

    @Test
    public void invalidType() {
      assertThatThrownBy(
              () -> {
                Values.string(Values.NULL);
              })
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Expected STRING value, received NULL");
    }
  }

  @Nested
  class UUIDTest {

    @Test
    public void encodeDecode() {
      UUID expected = UUID.randomUUID();
      assertThat(Values.uuid(Values.of(expected))).isEqualTo(expected);
    }

    @Test
    public void invalidType() {
      assertThatThrownBy(
              () -> {
                Values.uuid(Values.NULL);
              })
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Expected UUID value, received NULL");
    }

    @Test
    public void invalidBytes() {
      assertThatThrownBy(
              () -> {
                Values.uuid(
                    Value.newBuilder()
                        .setUuid(Uuid.newBuilder().setValue(ByteString.copyFrom(new byte[17])))
                        .build());
              })
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Expected 16 bytes for a uuid values");
    }
  }

  @Nested
  class InetTest {

    @Test
    public void encodeDecodeV4() throws UnknownHostException {
      InetAddress expected = InetAddress.getByName("127.0.0.1");
      assertThat(Values.inet(Values.of(expected))).isEqualTo(expected);
    }

    @Test
    public void encodeDecodeV6() throws UnknownHostException {
      InetAddress expected = InetAddress.getByName("2001:db8::8a2e:370:7334");
      assertThat(Values.inet(Values.of(expected))).isEqualTo(expected);
    }

    @Test
    public void invalidType() {
      assertThatThrownBy(
              () -> {
                Values.inet(Values.NULL);
              })
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Expected INET value, received NULL");
    }

    @Test
    public void invalidBytes() {
      assertThatThrownBy(
              () -> {
                Values.inet(
                    Value.newBuilder()
                        .setInet(Inet.newBuilder().setValue(ByteString.copyFrom(new byte[5])))
                        .build());
              })
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Expected 4 bytes (IPv4) or 16 (IPv6) bytes for a inet values");
    }
  }

  @Nested
  public class VarintTest {

    @Test
    public void encodeDecode() {
      BigInteger expected = new BigInteger("99999999999999999999999999999999999999");
      assertThat(Values.varint(Values.of(expected))).isEqualTo(expected);
    }

    @Test
    public void invalidType() {
      assertThatThrownBy(
              () -> {
                Values.varint(Values.NULL);
              })
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Expected VARINT value, received NULL");
    }
  }

  @Nested
  public class DecimalTest {

    @Test
    public void encodeDecode() {
      BigDecimal expected = new BigDecimal("9999999999999.9999999999999999999999999");
      assertThat(Values.decimal(Values.of(expected))).isEqualTo(expected);
    }

    @Test
    public void invalidType() {
      assertThatThrownBy(
              () -> {
                Values.decimal(Values.NULL);
              })
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Expected DECIMAL value, received NULL");
    }
  }

  @Nested
  public class DateTest {

    @Test
    public void encodeDecode() {
      LocalDate expected = LocalDate.now();
      assertThat(Values.date(Values.of(expected))).isEqualTo(expected);
    }

    @Test
    public void invalidType() {
      assertThatThrownBy(
              () -> {
                Values.date(Values.NULL);
              })
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Expected DATE value, received NULL");
    }
  }

  @Nested
  public class TimeTest {

    @Test
    public void encodeDecode() {
      LocalTime expected = LocalTime.now();
      assertThat(Values.time(Values.of(expected))).isEqualTo(expected);
    }

    @Test
    public void invalidType() {
      assertThatThrownBy(
              () -> {
                Values.time(Values.NULL);
              })
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Expected TIME value, received NULL");
    }
  }
}
