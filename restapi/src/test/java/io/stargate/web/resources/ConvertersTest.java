package io.stargate.web.resources;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.db.schema.Column.Type;
import io.stargate.db.schema.ImmutableColumn;
import io.stargate.db.schema.ImmutableUserDefinedType;
import io.stargate.db.schema.ParameterizedType.TupleType;
import io.stargate.db.schema.UserDefinedType;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Collections;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ConvertersTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @ParameterizedTest
  @MethodSource("samples")
  @DisplayName("Should coerce JSON value")
  public void coerceTest(ColumnType type, String json, Object expected) throws Exception {
    Object actual = Converters.coerce(type, OBJECT_MAPPER.readValue(json, Object.class));
    if (actual instanceof BigDecimal) {
      assertThat(((BigDecimal) actual)).isEqualByComparingTo((BigDecimal) expected);
    } else {
      assertThat(actual).isEqualTo(expected);
    }
  }

  private static Arguments[] samples() throws Exception {
    TupleType intTextTupleType = (TupleType) Type.Tuple.of(Type.Int, Type.Text);
    UserDefinedType addressUdt =
        ImmutableUserDefinedType.builder()
            .keyspace("directory")
            .name("address")
            .addColumns(
                ImmutableColumn.builder()
                    .name("street")
                    .type(Type.Text)
                    .kind(Column.Kind.Regular)
                    .build(),
                ImmutableColumn.builder()
                    .name("zip")
                    .type(Type.Int)
                    .kind(Column.Kind.Regular)
                    .build())
            .build();

    return new Arguments[] {
      // Primitives:
      arguments(Type.Text, "\"abc\"", "abc"),
      arguments(Type.Varchar, "\"abc\"", "abc"),
      arguments(Type.Ascii, "\"abc\"", "abc"),
      arguments(Type.Boolean, "true", true),
      arguments(Type.Boolean, "\"true\"", true),
      arguments(Type.Tinyint, "1", (byte) 1),
      arguments(Type.Tinyint, "\"1\"", (byte) 1),
      arguments(Type.Smallint, "1", (short) 1),
      arguments(Type.Smallint, "\"1\"", (short) 1),
      arguments(Type.Int, "1", 1),
      arguments(Type.Int, "\"1\"", 1),
      arguments(Type.Bigint, "1", 1L),
      arguments(Type.Bigint, "\"1\"", 1L),
      arguments(Type.Counter, "1", 1L),
      arguments(Type.Counter, "\"1\"", 1L),
      arguments(Type.Float, "1.0", 1.0f),
      arguments(Type.Float, "1", 1.0f),
      arguments(Type.Float, "\"1\"", 1.0f),
      arguments(Type.Float, "\"1.0\"", 1.0f),
      // Float loss of precision or range are ignored silently (similar to CQL):
      arguments(Type.Float, "676543.21", 676543.2f),
      arguments(Type.Float, Double.toString(Double.MAX_VALUE), Float.POSITIVE_INFINITY),
      arguments(Type.Double, "1.0", 1.0),
      arguments(Type.Double, "1", 1.0),
      arguments(Type.Double, "\"1\"", 1.0),
      arguments(Type.Double, "\"1.0\"", 1.0),
      arguments(Type.Varint, "1", BigInteger.ONE),
      arguments(Type.Varint, "\"1\"", BigInteger.ONE),
      arguments(Type.Decimal, "1.0", BigDecimal.ONE),
      arguments(Type.Decimal, "1", BigDecimal.ONE),
      arguments(Type.Decimal, "\"1\"", BigDecimal.ONE),
      arguments(Type.Decimal, "\"1.0\"", BigDecimal.ONE),
      arguments(
          Type.Uuid,
          "\"59e86020-3502-11eb-bc1b-53a4f383ccdf\"",
          UUID.fromString("59e86020-3502-11eb-bc1b-53a4f383ccdf")),
      arguments(
          Type.Timeuuid,
          "\"59e86020-3502-11eb-bc1b-53a4f383ccdf\"",
          UUID.fromString("59e86020-3502-11eb-bc1b-53a4f383ccdf")),
      arguments(Type.Blob, "\"/w==\"", Bytes.fromHexString("0xff")),
      arguments(Type.Inet, "\"127.0.0.1\"", InetAddress.getByName("127.0.0.1")),
      arguments(Type.Date, "\"2001-01-01\"", LocalDate.of(2001, 1, 1)),
      arguments(Type.Date, "2147483648", LocalDate.of(1970, 1, 1)),
      arguments(Type.Time, "\"00:01:00\"", LocalTime.of(0, 1, 0)),
      arguments(Type.Time, "0", LocalTime.of(0, 0, 0)),
      arguments(Type.Timestamp, "\"1970-01-01T00:00:00Z\"", Instant.ofEpochMilli(0)),
      arguments(Type.Timestamp, "0", Instant.ofEpochMilli(0)),
      arguments(Type.Duration, "\"1d\"", CqlDuration.newInstance(0, 1, 0)),

      // List:
      arguments(Type.List.of(Type.Int), "[]", Collections.emptyList()),
      arguments(Type.List.of(Type.Int), "[1,2,3]", ImmutableList.of(1, 2, 3)),
      // Legacy v1 behavior: the whole value is represented as a single JSON string.
      // We're pretty lenient with single quotes.
      arguments(Type.List.of(Type.Int), "\"[1,2,3]\"", ImmutableList.of(1, 2, 3)),
      arguments(Type.List.of(Type.Text), "\"['a','b','c']\"", ImmutableList.of("a", "b", "c")),
      arguments(Type.List.of(Type.Text), "\"[a,b,c]\"", ImmutableList.of("a", "b", "c")),
      arguments(
          Type.List.of(Type.Text), "\"['a','b,c','d''']\"", ImmutableList.of("a", "b,c", "d'")),

      // Set:
      arguments(Type.Set.of(Type.Int), "[]", Collections.emptySet()),
      arguments(Type.Set.of(Type.Int), "[1,2,3]", ImmutableSet.of(1, 2, 3)),
      arguments(Type.Set.of(Type.Int), "\"{}\"", Collections.emptySet()),
      arguments(Type.Set.of(Type.Int), "\"{1,2,3}\"", ImmutableSet.of(1, 2, 3)),
      arguments(Type.Set.of(Type.Int), "[\"1\", \"2\", \"3\"]", ImmutableSet.of(1, 2, 3)),

      // Map:
      arguments(Type.Map.of(Type.Int, Type.Text), "[]", Collections.emptyMap()),
      arguments(
          Type.Map.of(Type.Int, Type.Text),
          "[{\"key\": 1, \"value\": \"a\"}, {\"key\": 2, \"value\": \"b\"}]",
          ImmutableMap.of(1, "a", 2, "b")),
      arguments(Type.Map.of(Type.Int, Type.Text), "\"{}\"", Collections.emptyMap()),
      arguments(
          Type.Map.of(Type.Int, Type.Text), "\"{1:'a',2:'b'}\"", ImmutableMap.of(1, "a", 2, "b")),

      // Tuple:
      arguments(intTextTupleType, "[]", intTextTupleType.create()),
      arguments(intTextTupleType, "[1,\"a\"]", intTextTupleType.create(1, "a")),
      arguments(intTextTupleType, "\"()\"", intTextTupleType.create()),
      arguments(intTextTupleType, "\"(1,a)\"", intTextTupleType.create(1, "a")),

      // UDT:
      arguments(addressUdt, "{}", addressUdt.create()),
      arguments(
          addressUdt,
          "{\"street\": \"1600 Pennsylvania Avenue NW\", \"zip\": 20500}",
          addressUdt.create("1600 Pennsylvania Avenue NW", 20500)),
      arguments(addressUdt, "\"{}\"", addressUdt.create()),
      arguments(
          addressUdt,
          "\"{street: '1600 Pennsylvania Avenue NW', zip: 20500}\"",
          addressUdt.create("1600 Pennsylvania Avenue NW", 20500)),
    };
  }
}
