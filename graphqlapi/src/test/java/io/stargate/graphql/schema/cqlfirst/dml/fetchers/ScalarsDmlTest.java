package io.stargate.graphql.schema.cqlfirst.dml.fetchers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import graphql.ExecutionResult;
import io.stargate.db.datastore.Row;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ImmutableColumn;
import io.stargate.db.schema.ImmutableKeyspace;
import io.stargate.db.schema.ImmutableTable;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.Schema;
import io.stargate.db.schema.Table;
import io.stargate.graphql.schema.cqlfirst.dml.DmlTestBase;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ScalarsDmlTest extends DmlTestBase {
  public static final Table table = buildTable();
  public static final Keyspace keyspace =
      ImmutableKeyspace.builder().name("scalars_ks").addTables(table).build();
  private static final String UUID_REGEX =
      "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";

  public static final Map<Column.Type, Function<Object, Object>> toRowCellFn = buildRowToCellMap();

  private static Table buildTable() {
    ImmutableTable.Builder tableBuilder =
        ImmutableTable.builder()
            .keyspace("scalars_ks")
            .name("Scalars")
            .addColumns(
                ImmutableColumn.builder()
                    .keyspace("scalars_ks")
                    .table("Scalars")
                    .name("id")
                    .type(Column.Type.Int)
                    .kind(Column.Kind.PartitionKey)
                    .build());

    tableBuilder.addColumns(
        Arrays.stream(Column.Type.values())
            .filter(
                t ->
                    !t.isCollection()
                        && !t.isTuple()
                        && !t.isUserDefined()
                        && t != Column.Type.Point
                        && t != Column.Type.LineString
                        && t != Column.Type.Polygon)
            .map(ScalarsDmlTest::getColumn)
            .toArray(ImmutableColumn[]::new));

    return tableBuilder.build();
  }

  private static ImmutableColumn getColumn(Column.Type type) {
    return ImmutableColumn.builder()
        .keyspace("scalars_ks")
        .table("Scalars")
        .name(getName(type))
        .type(type)
        .kind(Column.Kind.Regular)
        .build();
  }

  @Override
  public Schema getCQLSchema() {
    return Schema.create(Collections.singleton(keyspace));
  }

  @ParameterizedTest
  @MethodSource("getValues")
  public void shouldParseScalarValues(Column.Type type, Object value, String expectedLiteral) {
    String mutation = "mutation { insertScalars(value: { %s:%s, id:1 }) { applied } }";
    String name = getName(type);
    String expectedCQL =
        String.format(
            "INSERT INTO scalars_ks.\"Scalars\" (id, %s) VALUES (1, %s)",
            name, expectedLiteral != null ? expectedLiteral : value.toString());

    assertQuery(String.format(mutation, name, toGraphQLValue(value)), expectedCQL);
  }

  @ParameterizedTest
  @MethodSource("getAdditionalLiterals")
  public void shouldParseAdditionalScalarValuesForDataTypes(
      Column.Type type, Object value, String expectedLiteral) {
    // Some data types support more than one type of input
    // For example, BigInt Scalar supports both GraphQL strings and int
    String mutation = "mutation { insertScalars(value: { %s:%s, id:1 }) { applied } }";
    String name = getName(type);
    String expectedCQL =
        String.format(
            "INSERT INTO scalars_ks.\"Scalars\" (id, %s) VALUES (1, %s)",
            name, expectedLiteral != null ? expectedLiteral : value.toString());

    assertQuery(String.format(mutation, name, toGraphQLValue(value)), expectedCQL);
  }

  @ParameterizedTest
  @MethodSource("getValues")
  public void shouldEncodeScalarValues(Column.Type type, Object value) {
    String name = getName(type);
    Row row = createRowForSingleValue(name, toRowCellValue(type, value));
    when(resultSet.currentPageRows()).thenReturn(Collections.singletonList(row));
    ExecutionResult result =
        executeGraphQl(String.format("query { Scalars { values { %s } } }", name));
    assertThat(result.getErrors()).isEmpty();
    assertThat(result.<Map<String, Object>>getData())
        .extractingByKey("Scalars", InstanceOfAssertFactories.MAP)
        .extractingByKey("values", InstanceOfAssertFactories.LIST)
        .singleElement()
        .asInstanceOf(InstanceOfAssertFactories.MAP)
        .extractingByKey(name)
        .isEqualTo(value);
  }

  @ParameterizedTest
  @MethodSource("getIncorrectValues")
  public void incorrectLiteralsForScalarsShouldResultInError(Column.Type type, Object value) {
    String mutation = "mutation { insertScalars(value: { %s:%s, id:1 }) { applied } }";
    assertError(String.format(mutation, getName(type), toGraphQLValue(value)), "Validation error");
  }

  @Test
  public void timeUuidTypeShouldSupportNowFunction() {
    long startTime = System.currentTimeMillis();
    long unixToGregorian = 12219292800000L;
    long ticksInMs = 10000L;

    String mutation =
        "mutation { insertScalars(value: { timeuuidvalue:\"now()\", id:1 }) { "
            + "applied, value { timeuuidvalue } } }";
    ExecutionResult result = executeGraphQl(mutation);
    assertThat(result.getErrors()).isEmpty();
    assertThat(getCapturedQueryString())
        .matches(
            String.format(
                "INSERT INTO scalars_ks.\"Scalars\" \\(id, timeuuidvalue\\) VALUES \\(1, %s\\)",
                UUID_REGEX));

    assertThat(result.<Map<String, Object>>getData())
        .extractingByKey("insertScalars", InstanceOfAssertFactories.MAP)
        .extractingByKey("value", InstanceOfAssertFactories.MAP)
        .extractingByKey("timeuuidvalue")
        .extracting(
            x -> UUID.fromString(x.toString()).timestamp() / ticksInMs - unixToGregorian,
            InstanceOfAssertFactories.LONG)
        .isGreaterThanOrEqualTo(startTime)
        .isLessThanOrEqualTo(System.currentTimeMillis());
  }

  @Test
  public void uuidTypeShouldSupportUuidFunction() {
    String mutation =
        "mutation { insertScalars(value: { uuidvalue:\"uuid()\", id:1 }) { "
            + "applied, value { uuidvalue } } }";
    ExecutionResult result = executeGraphQl(mutation);
    assertThat(result.getErrors()).isEmpty();
    assertThat(getCapturedQueryString())
        .matches(
            String.format(
                "INSERT INTO scalars_ks.\"Scalars\" \\(id, uuidvalue\\) VALUES \\(1, %s\\)",
                UUID_REGEX));

    assertThat(result.<Map<String, Object>>getData())
        .extractingByKey("insertScalars", InstanceOfAssertFactories.MAP)
        .extractingByKey("value", InstanceOfAssertFactories.MAP)
        .extractingByKey("uuidvalue")
        .extracting(x -> UUID.fromString(x.toString()).version())
        .isEqualTo(4);
  }

  @SuppressWarnings("unused") // referenced by @MethodSource
  private static Stream<Arguments> getValues() {
    String timestampLiteral =
        instantFormatter().format(Date.from(Instant.parse("2020-01-03T10:15:31.123Z")));

    return Stream.of(
        arguments(Column.Type.Ascii, "abc", "'abc'"),
        arguments(Column.Type.Ascii, "", "''"),
        arguments(Column.Type.Bigint, "-2147483648", null),
        arguments(Column.Type.Bigint, "-1", null),
        arguments(Column.Type.Bigint, "1", null),
        arguments(Column.Type.Bigint, "2147483647", null),
        arguments(Column.Type.Bigint, "-9223372036854775807", null),
        arguments(Column.Type.Bigint, "9223372036854775807", null),
        arguments(Column.Type.Bigint, "1", null),
        arguments(Column.Type.Blob, "AQID//7gEiMB", "0x010203fffee0122301"),
        arguments(Column.Type.Blob, "/w==", "0xff"),
        arguments(Column.Type.Boolean, true, null),
        arguments(Column.Type.Boolean, false, null),
        arguments(Column.Type.Date, "2005-08-05", "'2005-08-05'"),
        arguments(Column.Type.Date, "2010-04-29", "'2010-04-29'"),
        arguments(Column.Type.Decimal, "1.12", null),
        arguments(Column.Type.Decimal, "0.99", null),
        arguments(Column.Type.Decimal, "-0.123456", null),
        arguments(Column.Type.Double, -1D, null),
        arguments(Column.Type.Double, Double.MIN_VALUE, null),
        arguments(Column.Type.Double, Double.MAX_VALUE, null),
        arguments(Column.Type.Double, 12.666429, null),
        arguments(Column.Type.Double, 12.666428727762776, null),
        arguments(Column.Type.Duration, "12h30m", null),
        arguments(Column.Type.Float, 1.1234f, null),
        arguments(Column.Type.Float, 0f, null),
        arguments(Column.Type.Float, Float.MIN_VALUE, null),
        arguments(Column.Type.Float, Float.MAX_VALUE, null),
        arguments(Column.Type.Inet, "0:0:0:0:0:0:0:1", "'0:0:0:0:0:0:0:1'"),
        arguments(Column.Type.Inet, "2001:db8:a0b:12f0:0:0:0:1", "'2001:db8:a0b:12f0:0:0:0:1'"),
        arguments(Column.Type.Inet, "8.8.8.8", "'8.8.8.8'"),
        arguments(Column.Type.Inet, "127.0.0.1", "'127.0.0.1'"),
        arguments(Column.Type.Inet, "10.1.2.3", "'10.1.2.3'"),
        arguments(Column.Type.Int, 1, null),
        arguments(Column.Type.Int, -1, null),
        arguments(Column.Type.Int, 0, null),
        arguments(Column.Type.Int, Integer.MAX_VALUE, null),
        arguments(Column.Type.Int, Integer.MIN_VALUE, null),
        arguments(Column.Type.Smallint, 0L, null),
        arguments(Column.Type.Smallint, -1L, null),
        arguments(Column.Type.Smallint, 1L, null),
        arguments(Column.Type.Smallint, (long) Short.MAX_VALUE, null),
        arguments(Column.Type.Smallint, (long) Short.MIN_VALUE, null),
        arguments(Column.Type.Text, "abc123", "'abc123'"),
        arguments(Column.Type.Text, "", "''"),
        arguments(Column.Type.Time, "10:15:30.123456789", "'10:15:30.123456789'"),
        arguments(Column.Type.Timestamp, timestampLiteral, "'" + timestampLiteral + "'"),
        arguments(Column.Type.Tinyint, 0L, null),
        arguments(Column.Type.Tinyint, 1L, null),
        arguments(Column.Type.Tinyint, -1L, null),
        arguments(Column.Type.Tinyint, (long) Byte.MIN_VALUE, null),
        arguments(Column.Type.Tinyint, (long) Byte.MAX_VALUE, null),
        arguments(Column.Type.Timeuuid, "30821634-13ad-11eb-adc1-0242ac120002", null),
        arguments(Column.Type.Uuid, "f3abdfbf-479f-407b-9fde-128145bd7bef", null),
        arguments(Column.Type.Varchar, "abc123", "'abc123'"),
        arguments(Column.Type.Varchar, "", "''"),
        arguments(Column.Type.Varint, "0", null),
        arguments(Column.Type.Varint, "-1", null),
        arguments(Column.Type.Varint, "1", null),
        arguments(Column.Type.Varint, "92233720368547758070000", null));
  }

  @SuppressWarnings("unused") // referenced by @MethodSource
  private static Stream<Arguments> getAdditionalLiterals() {
    return Stream.of(
        arguments(Column.Type.Bigint, -1, null),
        arguments(Column.Type.Bigint, 1, null),
        arguments(Column.Type.Bigint, -2147483648, null),
        arguments(Column.Type.Bigint, 2147483648023L, null));
  }

  @SuppressWarnings("unused") // referenced by @MethodSource
  private static Stream<Arguments> getIncorrectValues() {
    return Stream.of(arguments(Column.Type.Bigint, "1.2"), arguments(Column.Type.Bigint, "ABC"));
  }

  private static Object toDbBigInt(Object literal) {
    if (literal instanceof String) {
      return Long.valueOf((String) literal);
    }
    return literal;
  }

  private static Object toRowCellValue(Column.Type type, Object literal) {
    Function<Object, Object> fn = toRowCellFn.get(type);
    if (fn == null) {
      return literal;
    }
    return fn.apply(literal);
  }

  private static String toGraphQLValue(Object value) {
    if (value instanceof String) {
      return String.format("\"%s\"", value);
    }

    return value.toString();
  }

  /** Gets the column name for a scalar type. */
  private static String getName(Column.Type type) {
    return type.name().toLowerCase() + "value";
  }

  private Row createRowForSingleValue(String columnName, Object value) {
    Map<String, Object> values = new HashMap<>();
    values.put(columnName, value);
    return createRow(Collections.singletonList(table.column(columnName)), values);
  }

  private static Map<Column.Type, Function<Object, Object>> buildRowToCellMap() {
    // Converters to mock row data
    return ImmutableMap.<Column.Type, Function<Object, Object>>builder()
        .put(Column.Type.Bigint, ScalarsDmlTest::toDbBigInt)
        .put(Column.Type.Blob, o -> ByteBuffer.wrap(Base64.getDecoder().decode(o.toString())))
        .put(Column.Type.Date, o -> LocalDate.parse(o.toString()))
        .put(Column.Type.Decimal, o -> new BigDecimal(o.toString()))
        .put(Column.Type.Duration, o -> CqlDuration.from(o.toString()))
        .put(
            Column.Type.Inet,
            o -> {
              try {
                return InetAddress.getByName(o.toString());
              } catch (UnknownHostException e) {
                throw new RuntimeException(e);
              }
            })
        .put(Column.Type.Smallint, o -> Short.valueOf(o.toString()))
        .put(Column.Type.Time, o -> LocalTime.parse(o.toString()))
        .put(
            Column.Type.Timestamp,
            o -> {
              try {
                return instantFormatter().parse(o.toString()).toInstant();
              } catch (ParseException e) {
                throw new RuntimeException(e);
              }
            })
        .put(Column.Type.Tinyint, o -> Byte.valueOf(o.toString()))
        .put(Column.Type.Timeuuid, o -> UUID.fromString(o.toString()))
        .put(Column.Type.Uuid, o -> UUID.fromString(o.toString()))
        .put(Column.Type.Varint, o -> new BigInteger(o.toString()))
        .build();
  }

  private static SimpleDateFormat instantFormatter() {
    SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    dateFormatter.setTimeZone(TimeZone.getTimeZone(ZoneId.systemDefault()));
    return dateFormatter;
  }
}
