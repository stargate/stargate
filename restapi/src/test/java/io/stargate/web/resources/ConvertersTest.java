package io.stargate.web.resources;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.db.query.Modification.Operation;
import io.stargate.db.query.builder.Value;
import io.stargate.db.query.builder.ValueModifier;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.db.schema.Column.Kind;
import io.stargate.db.schema.Column.Type;
import io.stargate.db.schema.ImmutableColumn;
import io.stargate.db.schema.ImmutableTable;
import io.stargate.db.schema.ImmutableUserDefinedType;
import io.stargate.db.schema.ParameterizedType.TupleType;
import io.stargate.db.schema.Table;
import io.stargate.db.schema.UserDefinedType;
import io.stargate.web.impl.RestApiServer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Collections;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ConvertersTest {

  private static final ObjectMapper OBJECT_MAPPER = testObjectMapper();
  private static final TupleType INT_TEXT_TUPLE = (TupleType) Type.Tuple.of(Type.Int, Type.Text);
  private static final UserDefinedType ADDRESS_UDT =
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
  private static final Table COUNTER_TABLE =
      ImmutableTable.builder()
          .keyspace("library")
          .name("book_popular_count")
          .addColumns(
              ImmutableColumn.builder()
                  .keyspace("library")
                  .table("book_popular_count")
                  .name("id")
                  .type(Type.Text)
                  .kind(Kind.PartitionKey)
                  .build(),
              ImmutableColumn.builder()
                  .keyspace("library")
                  .table("book_popular_count")
                  .name("popularity")
                  .type(Type.Counter)
                  .kind(Kind.Regular)
                  .build())
          .build();

  @ParameterizedTest
  @MethodSource("toJsonSamples")
  @DisplayName("Should coerce CQL value to JSON")
  public void toJsonValueTest(Object cqlValue, String expectedJson) throws JsonProcessingException {
    String actualJson = OBJECT_MAPPER.writeValueAsString(Converters.toJsonValue(cqlValue));
    assertThat(actualJson).isEqualTo(expectedJson);
  }

  @SuppressWarnings("unused") // referenced by @MethodSource
  private static Arguments[] toJsonSamples() throws Exception {
    return new Arguments[] {
      arguments(
          UUID.fromString("59e86020-3502-11eb-bc1b-53a4f383ccdf"),
          "\"59e86020-3502-11eb-bc1b-53a4f383ccdf\""),
      arguments("abc", "\"abc\""),
      arguments(CqlDuration.newInstance(0, 1, 0), "\"1d\""),
      arguments(1L, "\"1\""),
      arguments(BigInteger.ONE, "\"1\""),
      arguments(BigDecimal.ONE, "\"1\""),
      arguments(Bytes.fromHexString("0xff"), "\"/w==\""),
      arguments(InetAddress.getByName("127.0.0.1"), "\"127.0.0.1\""),
      arguments(Collections.emptyList(), "[]"),
      arguments(ImmutableList.of(1, 2, 3), "[1,2,3]"),
      arguments(Collections.emptySet(), "[]"),
      arguments(ImmutableSet.of(1, 2, 3), "[1,2,3]"),
      arguments(Collections.emptyMap(), "[]"),
      arguments(
          ImmutableMap.of(1, "a", 2, "b"),
          "[{\"key\":1,\"value\":\"a\"},{\"key\":2,\"value\":\"b\"}]"),
      arguments(INT_TEXT_TUPLE.create(1, "a"), "[1,\"a\"]"),
      arguments(
          ADDRESS_UDT.create("1600 Pennsylvania Avenue NW", 20500),
          "{\"street\":\"1600 Pennsylvania Avenue NW\",\"zip\":20500}"),
      arguments("abc", "\"abc\""),
      arguments(true, "true"),
      arguments((byte) 1, "1"),
      arguments((short) 1, "1"),
      arguments(1, "1"),
      arguments(1, "1"),
      arguments(LocalDate.of(2001, 1, 1), "\"2001-01-01\""),
      arguments(LocalTime.of(0, 1, 0), "\"00:01:00\""),
      arguments(Instant.ofEpochMilli(0), "\"1970-01-01T00:00:00Z\""),
    };
  }

  @ParameterizedTest
  @MethodSource("toCqlSamples")
  @DisplayName("Should coerce JSON value to CQL")
  public void toCqlValueTest(ColumnType type, String json, Object expected) throws Exception {
    Object actual = Converters.toCqlValue(type, OBJECT_MAPPER.readValue(json, Object.class));
    if (actual instanceof BigDecimal) {
      assertThat(((BigDecimal) actual)).isEqualByComparingTo((BigDecimal) expected);
    } else {
      assertThat(actual).isEqualTo(expected);
    }
  }

  @SuppressWarnings("unused") // referenced by @MethodSource
  private static Arguments[] toCqlSamples() throws Exception {
    return new Arguments[] {
      // Primitives:
      arguments(Type.Text, "\"abc\"", "abc"),
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
      arguments(INT_TEXT_TUPLE, "[]", INT_TEXT_TUPLE.create()),
      arguments(INT_TEXT_TUPLE, "[1,\"a\"]", INT_TEXT_TUPLE.create(1, "a")),
      arguments(INT_TEXT_TUPLE, "\"()\"", INT_TEXT_TUPLE.create()),
      arguments(INT_TEXT_TUPLE, "\"(1,a)\"", INT_TEXT_TUPLE.create(1, "a")),

      // UDT:
      arguments(ADDRESS_UDT, "{}", ADDRESS_UDT.create()),
      arguments(
          ADDRESS_UDT,
          "{\"street\": \"1600 Pennsylvania Avenue NW\", \"zip\": 20500}",
          ADDRESS_UDT.create("1600 Pennsylvania Avenue NW", 20500)),
      arguments(ADDRESS_UDT, "\"{}\"", ADDRESS_UDT.create()),
      arguments(
          ADDRESS_UDT,
          "\"{street: '1600 Pennsylvania Avenue NW', zip: 20500}\"",
          ADDRESS_UDT.create("1600 Pennsylvania Avenue NW", 20500)),
    };
  }

  @ParameterizedTest
  @MethodSource("toCqlErrorSamples")
  @DisplayName("Should fail to coerce invalid JSON value to CQL")
  public void toCqlValueTest(ColumnType type, String json, String expectedError) throws Exception {
    assertThatThrownBy(
            () -> Converters.toCqlValue(type, OBJECT_MAPPER.readValue(json, Object.class)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(expectedError);
  }

  @SuppressWarnings("unused") // referenced by @MethodSource
  private static Arguments[] toCqlErrorSamples() {
    return new Arguments[] {
      arguments(Type.Text, "1", "Invalid Text value '1': expected a string"),
      arguments(Type.Ascii, "true", "Invalid Ascii value 'true': expected a string"),
      arguments(Type.Boolean, "1", "Invalid Boolean value '1': expected a boolean or a string"),
      arguments(Type.Boolean, "\"a\"", "Invalid Boolean value 'a': cannot parse"),
      arguments(
          Type.Tinyint, "true", "Invalid Tinyint value 'true': expected an integer or a string"),
      arguments(Type.Tinyint, "128", "Invalid Tinyint value '128': out of range"),
      arguments(Type.Tinyint, "\"a\"", "Invalid Tinyint value 'a': cannot parse"),
      arguments(
          Type.Smallint, "[]", "Invalid Smallint value '[]': expected an integer or a string"),
      arguments(Type.Smallint, "32768", "Invalid Smallint value '32768': out of range"),
      arguments(Type.Smallint, "\"a\"", "Invalid Smallint value 'a': cannot parse"),
      arguments(Type.Int, "{}", "Invalid Int value '{}': expected an integer or a string"),
      arguments(Type.Int, "2147483648", "Invalid Int value '2147483648': out of range"),
      arguments(Type.Int, "\"a\"", "Invalid Int value 'a': cannot parse"),
      arguments(Type.Bigint, "{}", "Invalid Bigint value '{}': expected an integer or a string"),
      arguments(
          Type.Bigint,
          "9223372036854775808",
          "Invalid Bigint value '9223372036854775808': out of range"),
      arguments(Type.Bigint, "\"a\"", "Invalid Bigint value 'a': cannot parse"),
      arguments(Type.Counter, "{}", "Invalid Counter value '{}': expected an integer or a string"),
      arguments(
          Type.Counter,
          "9223372036854775808",
          "Invalid Counter value '9223372036854775808': out of range"),
      arguments(Type.Counter, "\"a\"", "Invalid Counter value 'a': cannot parse"),
      arguments(
          Type.Varint, "true", "Invalid Varint value 'true': expected an integer or a string"),
      arguments(Type.Varint, "\"a\"", "Invalid Varint value 'a': cannot parse"),
      arguments(Type.Float, "true", "Invalid Float value 'true': expected a number or a string"),
      arguments(Type.Float, "\"a\"", "Invalid Float value 'a': cannot parse"),
      arguments(Type.Double, "true", "Invalid Double value 'true': expected a number or a string"),
      arguments(Type.Double, "\"a\"", "Invalid Double value 'a': cannot parse"),
      arguments(
          Type.Decimal, "true", "Invalid Decimal value 'true': expected a number or a string"),
      arguments(Type.Decimal, "\"a\"", "Invalid Decimal value 'a': cannot parse"),
      arguments(Type.Uuid, "1", "Invalid Uuid value '1': expected a string"),
      arguments(Type.Uuid, "\"a\"", "Invalid Uuid value 'a': cannot parse"),
      arguments(Type.Timeuuid, "1", "Invalid Timeuuid value '1': expected a string"),
      arguments(Type.Timeuuid, "\"a\"", "Invalid Timeuuid value 'a': cannot parse"),
      arguments(Type.Blob, "1", "Invalid Blob value '1': expected a string"),
      arguments(Type.Blob, "\"a\"", "Invalid Blob value 'a': cannot parse"),
      arguments(Type.Inet, "1", "Invalid Inet value '1': expected a string"),
      arguments(Type.Inet, "\"a\"", "Invalid Inet value a"),
      arguments(Type.Date, "true", "Invalid Date value 'true': expected an integer or a string"),
      arguments(Type.Date, "\"a\"", "Invalid Date value: Text 'a' could not be parsed at index 0"),
      arguments(Type.Time, "true", "Invalid Time value 'true': expected an integer or a string"),
      arguments(
          Type.Time,
          "\"11:20Z\"",
          "Invalid Time value: Text '11:20Z' could not be parsed, unparsed text found at index 5"),
      arguments(
          Type.Timestamp, "1.0", "Invalid Timestamp value '1.0': expected an integer or a string"),
      arguments(
          Type.Timestamp,
          "\"a\"",
          "Invalid Timestamp value: Text 'a' could not be parsed at index 0"),
      arguments(
          Type.List.of(Type.Int), "1", "Invalid List value '1': expected a JSON array or a string"),
      arguments(
          Type.List.of(Type.Int),
          "[\"a\"]",
          "Invalid Int value 'a': cannot parse (at index 0 of List '[a]')"),
      arguments(
          Type.List.of(Type.Int),
          "\"[1,2,3\"",
          "Invalid List value '[1,2,3': at character 6 expecting ',' or ']' but got EOF"),
      arguments(
          Type.List.of(Type.Int),
          "\"\"",
          "Invalid List value '': at character 0 expecting '[' but got EOF"),
      arguments(
          Type.Set.of(Type.Int), "1", "Invalid Set value '1': expected a JSON array or a string"),
      arguments(
          Type.Set.of(Type.Int),
          "[1, \"a\"]",
          "Invalid Int value 'a': cannot parse (at index 1 of Set '[1, a]')"),
      arguments(
          Type.Set.of(Type.Int),
          "\"[1]\"",
          "Invalid Set value '[1]': at character 1 expecting '{' but got '1'"),
      arguments(
          Type.Map.of(Type.Int, Type.Text),
          "1",
          "Invalid Map value '1': expected a JSON array of key/value objects, or a string"),
      arguments(
          Type.Map.of(Type.Int, Type.Text),
          "[1]",
          "Invalid Map value '[1]': inner elements must be objects with two fields 'key' and 'value'"),
      arguments(
          Type.Map.of(Type.Int, Type.Text),
          "[{\"key\": 1, \"value\": 1}]",
          "Invalid Text value '1': expected a string (value at index 0 of Map '[{key=1, value=1}]')"),
      arguments(
          Type.Map.of(Type.Int, Type.Text),
          "[{\"key\": \"a\", \"value\": \"a\"}]",
          "Invalid Int value 'a': cannot parse (key at index 0 of Map '[{key=a, value=a}]')"),
      arguments(
          Type.Map.of(Type.Int, Type.Text),
          "\"{1:a, 2}\"",
          "Invalid map value '{1:a, 2}': at character 7 expecting ':' but got '}'"),
      arguments(
          Type.Map.of(Type.Int, Type.Text),
          "\"\"",
          "Invalid Map value '': at character 0 expecting '{' but got EOF"),
      arguments(INT_TEXT_TUPLE, "1", "Invalid Tuple value '1': expected a JSON array or a string"),
      arguments(
          INT_TEXT_TUPLE,
          "[\"a\"]",
          "Invalid Int value 'a': cannot parse (field at index 0 of Tuple '[a]')"),
      arguments(
          INT_TEXT_TUPLE,
          "\"(1,a\"",
          "Invalid tuple value '(1,a': at field 1 (character 4) expecting ',' or ')', but got EOF"),
      arguments(
          INT_TEXT_TUPLE,
          "\"\"",
          "Invalid Tuple value '': at character 0 expecting '(' but got EOF"),
      arguments(ADDRESS_UDT, "[]", "Invalid UDT value '[]': expected a JSON object or a string"),
      arguments(
          ADDRESS_UDT,
          "{\"street\": 1, \"zip\": 1}",
          "Invalid Text value '1': expected a string (field 'street' of UDT '{street=1, zip=1}')"),
      arguments(
          ADDRESS_UDT,
          "{\"name\": \"wrong\"}",
          "Invalid UDT value '{name=wrong}': unknown field name \"name\""),
      arguments(
          ADDRESS_UDT,
          "\"{street}\"",
          "Invalid UDT value '{street}': at field street (character 7) expecting ':', but got '}'"),
      arguments(
          ADDRESS_UDT,
          "\"{name: 'wrong'}\"",
          "Invalid UDT value '{name: 'wrong'}': unknown field name at character 5: \"name\""),
      arguments(
          ADDRESS_UDT,
          "\"{\"",
          "Invalid UDT value '{': at character 1 expecting CQL identifier or '}', got EOF"),
      arguments(
          ADDRESS_UDT, "\"\"", "Invalid UDT value '': at character 0 expecting '{' but got EOF"),
    };
  }

  private static ObjectMapper testObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    RestApiServer.configureObjectMapper(mapper);
    return mapper;
  }

  @Test
  public void colToValuePositiveCounter() {
    ValueModifier modifier = Converters.colToValue("popularity", "+1", COUNTER_TABLE);
    ValueModifier expected = ValueModifier.of("popularity", Value.of(1L), Operation.INCREMENT);

    assertThat(modifier).isEqualTo(expected);
  }

  @Test
  public void colToValueCounterWithoutPlus() {
    ValueModifier modifier = Converters.colToValue("popularity", "1", COUNTER_TABLE);
    ValueModifier expected = ValueModifier.of("popularity", Value.of(1L), Operation.INCREMENT);

    assertThat(modifier).isEqualTo(expected);
  }

  @Test
  public void colToValueNegativeCounter() {
    ValueModifier modifier = Converters.colToValue("popularity", "-1", COUNTER_TABLE);
    ValueModifier expected = ValueModifier.of("popularity", Value.of(-1L), Operation.INCREMENT);

    assertThat(modifier).isEqualTo(expected);
  }

  @Test
  public void colToValueZeroCounter() {
    ValueModifier modifier = Converters.colToValue("popularity", "0", COUNTER_TABLE);
    ValueModifier expected = ValueModifier.of("popularity", Value.of(0L), Operation.INCREMENT);

    assertThat(modifier).isEqualTo(expected);
  }

  @Test
  public void colToValueBadCounter() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> Converters.colToValue("popularity", "a", COUNTER_TABLE));
    assertThat(ex).hasMessage("Invalid Counter value 'a': cannot parse");
  }
}
