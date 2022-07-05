package io.stargate.sgv2.graphql.schema.cqlfirst.dml.fetchers;

import static io.stargate.sgv2.graphql.schema.cqlfirst.dml.fetchers.ScalarsDmlTest.ScalarValue.v;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.jayway.jsonpath.JsonPath;
import graphql.ExecutionResult;
import io.stargate.bridge.grpc.CqlDuration;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass.ColumnSpec;
import io.stargate.bridge.proto.QueryOuterClass.TypeSpec;
import io.stargate.bridge.proto.QueryOuterClass.Value;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.graphql.schema.SampleKeyspaces;
import io.stargate.sgv2.graphql.schema.cqlfirst.dml.DmlTestBase;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.codec.binary.Hex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class ScalarsDmlTest extends DmlTestBase {

  @Override
  protected List<Schema.CqlKeyspaceDescribe> getCqlSchema() {
    return ImmutableList.of(SampleKeyspaces.SCALARS);
  }

  @ParameterizedTest
  @MethodSource("getValues")
  public void shouldInsertScalarValue(ScalarValue value) {
    String graphql =
        String.format(
            "mutation { insertScalars(value: { %s:%s, id:1 }) { applied } }",
            value.columnName(), value.graphqlLiteral);
    String expectedCQL =
        String.format(
            "INSERT INTO scalars_ks.\"Scalars\" (id, %s) VALUES (?, ?)", value.columnName());
    assertQuery(graphql, expectedCQL, ImmutableList.of(Values.of(1), value.grpc));
  }

  @ParameterizedTest
  @MethodSource("getValues")
  public void shouldQueryScalarValue(ScalarValue value) {
    String graphql = String.format("query { Scalars { values { %s } } }", value.columnName());
    mockResultSet(singleRowResultSet(value.columnSpec(), value.grpc));
    ExecutionResult result = executeGraphql(graphql);
    assertThat(result.getErrors()).isEmpty();

    Map<String, Object> data = result.getData();
    String jsonPath = String.format("$.Scalars.values[0].%s", value.columnName());
    assertThat(JsonPath.<Object>read(data, jsonPath)).isEqualTo(value.graphql);
  }

  @ParameterizedTest
  @MethodSource("getIncorrectValues")
  public void shouldFailOnInvalidValue(ScalarValue value) {
    String graphql =
        String.format(
            "mutation { insertScalars(value: { %s:%s, id:1 }) { applied } }",
            value.columnName(), value.graphqlLiteral);
    assertError(graphql, "Validation error");
  }

  @Test
  public void timeUuidTypeShouldSupportNowFunction() {
    long startTime = System.currentTimeMillis();

    String graphql =
        "mutation { insertScalars(value: { timeuuidvalue:\"now()\", id:1 }) { "
            + "applied, value { timeuuidvalue } } }";
    ExecutionResult result = executeGraphql(graphql);

    assertThat(getCapturedCql())
        .isEqualTo("INSERT INTO scalars_ks.\"Scalars\" (id, timeuuidvalue) " + "VALUES (?, ?)");
    List<Value> values = getCapturedValues();
    assertThat(values).hasSize(2);
    assertThat(values.get(0)).isEqualTo(Values.of(1));

    UUID uuid = Values.uuid(values.get(1));
    assertThat(uuid.version()).isEqualTo(1);
    assertThat(getJavaTimestamp(uuid))
        .isGreaterThanOrEqualTo(startTime)
        .isLessThanOrEqualTo(System.currentTimeMillis());

    assertThat(result.getErrors()).isEmpty();
    Map<String, Object> data = result.getData();
    assertThat(JsonPath.<String>read(data, "$.insertScalars.value.timeuuidvalue"))
        .isEqualTo(uuid.toString());
  }

  private long getJavaTimestamp(UUID uuid) {
    long unixToGregorian = 12219292800000L;
    long ticksInMs = 10000L;
    return uuid.timestamp() / ticksInMs - unixToGregorian;
  }

  @Test
  public void uuidTypeShouldSupportUuidFunction() {
    String graphql =
        "mutation { insertScalars(value: { uuidvalue:\"uuid()\", id:1 }) { "
            + "applied, value { uuidvalue } } }";
    ExecutionResult result = executeGraphql(graphql);

    assertThat(getCapturedCql())
        .isEqualTo("INSERT INTO scalars_ks.\"Scalars\" (id, uuidvalue) " + "VALUES (?, ?)");
    List<Value> values = getCapturedValues();
    assertThat(values).hasSize(2);
    assertThat(values.get(0)).isEqualTo(Values.of(1));

    UUID uuid = Values.uuid(values.get(1));
    assertThat(uuid.version()).isEqualTo(4);

    assertThat(result.getErrors()).isEmpty();
    Map<String, Object> data = result.getData();
    assertThat(JsonPath.<String>read(data, "$.insertScalars.value.uuidvalue"))
        .isEqualTo(uuid.toString());
  }

  @SuppressWarnings("unused") // referenced by @MethodSource
  private static ScalarValue[] getValues() throws Exception {

    Instant timestamp = Instant.parse("2020-01-03T10:15:31.123Z");
    String timestampGraphql =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
            .withZone(ZoneOffset.systemDefault())
            .format(timestamp);
    Value timestampGrpc = Values.of(timestamp.toEpochMilli());

    return new ScalarValue[] {
      v(TypeSpec.Basic.ASCII, "\"abc\"", "abc", Values.of("abc")),
      v(TypeSpec.Basic.ASCII, "\"\"", "", Values.of("")),
      v(TypeSpec.Basic.BIGINT, "-2147483648", "-2147483648", Values.of(-2147483648L)),
      v(TypeSpec.Basic.BIGINT, "-1", "-1", Values.of(-1L)),
      v(TypeSpec.Basic.BIGINT, "1", "1", Values.of(1L)),
      v(TypeSpec.Basic.BIGINT, "2147483647", "2147483647", Values.of(2147483647L)),
      v(
          TypeSpec.Basic.BIGINT,
          "-9223372036854775807",
          "-9223372036854775807",
          Values.of(-9223372036854775807L)),
      v(
          TypeSpec.Basic.BIGINT,
          "9223372036854775807",
          "9223372036854775807",
          Values.of(9223372036854775807L)),
      // BIGINT also support string inputs:
      v(TypeSpec.Basic.BIGINT, "\"-2147483648\"", "-2147483648", Values.of(-2147483648L)),
      v(TypeSpec.Basic.BIGINT, "\"-1\"", "-1", Values.of(-1L)),
      v(TypeSpec.Basic.BIGINT, "\"1\"", "1", Values.of(1L)),
      v(TypeSpec.Basic.BIGINT, "\"2147483647\"", "2147483647", Values.of(2147483647L)),
      v(
          TypeSpec.Basic.BLOB,
          "\"AQID//7gEiMB\"",
          "AQID//7gEiMB",
          Values.of(Hex.decodeHex("010203fffee0122301"))),
      v(TypeSpec.Basic.BLOB, "\"/w==\"", "/w==", Values.of(Hex.decodeHex("ff"))),
      v(TypeSpec.Basic.BOOLEAN, "true", true, Values.of(true)),
      v(TypeSpec.Basic.BOOLEAN, "false", false, Values.of(false)),
      v(
          TypeSpec.Basic.DATE,
          "\"2005-08-05\"",
          "2005-08-05",
          Values.of(LocalDate.parse("2005-08-05"))),
      v(
          TypeSpec.Basic.DATE,
          "\"2010-04-29\"",
          "2010-04-29",
          Values.of(LocalDate.parse("2010-04-29"))),
      v(TypeSpec.Basic.DECIMAL, "\"1.12\"", "1.12", Values.of(new BigDecimal("1.12"))),
      v(TypeSpec.Basic.DECIMAL, "\"0.99\"", "0.99", Values.of(new BigDecimal("0.99"))),
      v(
          TypeSpec.Basic.DECIMAL,
          "\"-0.123456\"",
          "-0.123456",
          Values.of(new BigDecimal("-0.123456"))),
      v(TypeSpec.Basic.DOUBLE, "-1", -1D, Values.of(-1D)),
      v(
          TypeSpec.Basic.DOUBLE,
          Double.toString(Double.MIN_VALUE),
          Double.MIN_VALUE,
          Values.of(Double.MIN_VALUE)),
      v(
          TypeSpec.Basic.DOUBLE,
          Double.toString(Double.MAX_VALUE),
          Double.MAX_VALUE,
          Values.of(Double.MAX_VALUE)),
      v(TypeSpec.Basic.DOUBLE, "12.666429", 12.666429, Values.of(12.666429)),
      v(
          TypeSpec.Basic.DOUBLE,
          "12.666428727762776",
          12.666428727762776,
          Values.of(12.666428727762776)),
      v(TypeSpec.Basic.DURATION, "\"12h30m\"", "12h30m", Values.of(CqlDuration.from("12h30m"))),
      v(TypeSpec.Basic.FLOAT, "1.1234", 1.1234f, Values.of(1.1234f)),
      v(TypeSpec.Basic.FLOAT, "0", 0f, Values.of(0f)),
      v(
          TypeSpec.Basic.FLOAT,
          Float.toString(Float.MIN_VALUE),
          Float.MIN_VALUE,
          Values.of(Float.MIN_VALUE)),
      v(
          TypeSpec.Basic.FLOAT,
          Float.toString(Float.MAX_VALUE),
          Float.MAX_VALUE,
          Values.of(Float.MAX_VALUE)),
      v(
          TypeSpec.Basic.INET,
          "\"0:0:0:0:0:0:0:1\"",
          "0:0:0:0:0:0:0:1",
          Values.of(InetAddress.getByName("0:0:0:0:0:0:0:1"))),
      v(
          TypeSpec.Basic.INET,
          "\"2001:db8:a0b:12f0:0:0:0:1\"",
          "2001:db8:a0b:12f0:0:0:0:1",
          Values.of(InetAddress.getByName("2001:db8:a0b:12f0:0:0:0:1"))),
      v(TypeSpec.Basic.INET, "\"8.8.8.8\"", "8.8.8.8", Values.of(InetAddress.getByName("8.8.8.8"))),
      v(
          TypeSpec.Basic.INET,
          "\"127.0.0.1\"",
          "127.0.0.1",
          Values.of(InetAddress.getByName("127.0.0.1"))),
      v(
          TypeSpec.Basic.INET,
          "\"10.1.2.3\"",
          "10.1.2.3",
          Values.of(InetAddress.getByName("10.1.2.3"))),
      v(TypeSpec.Basic.INT, "1", 1, Values.of(1)),
      v(TypeSpec.Basic.INT, "-1", -1, Values.of(-1)),
      v(TypeSpec.Basic.INT, "0", 0, Values.of(0)),
      v(
          TypeSpec.Basic.INT,
          Integer.toString(Integer.MAX_VALUE),
          Integer.MAX_VALUE,
          Values.of(Integer.MAX_VALUE)),
      v(
          TypeSpec.Basic.INT,
          Integer.toString(Integer.MIN_VALUE),
          Integer.MIN_VALUE,
          Values.of(Integer.MIN_VALUE)),
      v(TypeSpec.Basic.SMALLINT, "0", 0L, Values.of(0)),
      v(TypeSpec.Basic.SMALLINT, "-1", -1L, Values.of(-1)),
      v(TypeSpec.Basic.SMALLINT, "1", 1L, Values.of(1)),
      v(
          TypeSpec.Basic.SMALLINT,
          Short.toString(Short.MAX_VALUE),
          (long) Short.MAX_VALUE,
          Values.of(Short.MAX_VALUE)),
      v(
          TypeSpec.Basic.SMALLINT,
          Short.toString(Short.MIN_VALUE),
          (long) Short.MIN_VALUE,
          Values.of(Short.MIN_VALUE)),
      v(TypeSpec.Basic.VARCHAR, "\"abc123\"", "abc123", Values.of("abc123")),
      v(TypeSpec.Basic.VARCHAR, "\"\"", "", Values.of("")),
      v(
          TypeSpec.Basic.TIME,
          "\"10:15:30.123456789\"",
          "10:15:30.123456789",
          Values.of(LocalTime.parse("10:15:30.123456789"))),
      v(TypeSpec.Basic.TIMESTAMP, "\"" + timestampGraphql + "\"", timestampGraphql, timestampGrpc),
      v(TypeSpec.Basic.TINYINT, "0", 0L, Values.of(0)),
      v(TypeSpec.Basic.TINYINT, "1", 1L, Values.of(1)),
      v(TypeSpec.Basic.TINYINT, "-1", -1L, Values.of(-1)),
      v(
          TypeSpec.Basic.TINYINT,
          Byte.toString(Byte.MIN_VALUE),
          (long) Byte.MIN_VALUE,
          Values.of(Byte.MIN_VALUE)),
      v(
          TypeSpec.Basic.TINYINT,
          Byte.toString(Byte.MAX_VALUE),
          (long) Byte.MAX_VALUE,
          Values.of(Byte.MAX_VALUE)),
      v(
          TypeSpec.Basic.TIMEUUID,
          "\"30821634-13ad-11eb-adc1-0242ac120002\"",
          "30821634-13ad-11eb-adc1-0242ac120002",
          Values.of(UUID.fromString("30821634-13ad-11eb-adc1-0242ac120002"))),
      v(
          TypeSpec.Basic.UUID,
          "\"f3abdfbf-479f-407b-9fde-128145bd7bef\"",
          "f3abdfbf-479f-407b-9fde-128145bd7bef",
          Values.of(UUID.fromString("f3abdfbf-479f-407b-9fde-128145bd7bef"))),
      v(TypeSpec.Basic.VARINT, "0", "0", Values.of(BigInteger.ZERO)),
      v(TypeSpec.Basic.VARINT, "-1", "-1", Values.of(BigInteger.ONE.negate())),
      v(TypeSpec.Basic.VARINT, "1", "1", Values.of(BigInteger.ONE)),
      v(
          TypeSpec.Basic.VARINT,
          "92233720368547758070000",
          "92233720368547758070000",
          Values.of(new BigInteger("92233720368547758070000"))),
    };
  }

  @SuppressWarnings("unused") // referenced by @MethodSource
  private static ScalarValue[] getIncorrectValues() {
    return new ScalarValue[] {
      v(TypeSpec.Basic.BIGINT, "1.2"), v(TypeSpec.Basic.BIGINT, "ABC"),
    };
  }

  static class ScalarValue {

    static ScalarValue v(
        TypeSpec.Basic basicType, String graphqlLiteral, Object graphql, Value grpc) {
      return new ScalarValue(basicType, graphqlLiteral, graphql, grpc);
    }

    static ScalarValue v(TypeSpec.Basic basicType, String graphqlLiteral) {
      return new ScalarValue(basicType, graphqlLiteral, null, null);
    }

    final TypeSpec.Basic basicType;

    /** Representation that can be concatenated directly into a GraphQL query */
    final String graphqlLiteral;

    /** Representation passed to/from the GraphQL runtime */
    final Object graphql;

    /** Representation passed to/from the bridge */
    final Value grpc;

    ScalarValue(TypeSpec.Basic basicType, String graphqlLiteral, Object graphql, Value grpc) {
      this.basicType = basicType;
      this.graphql = graphql;
      this.graphqlLiteral = graphqlLiteral;
      this.grpc = grpc;
    }

    /** Name of the column of that type in {@link SampleKeyspaces#SCALARS} */
    String columnName() {
      return SampleKeyspaces.getScalarColumnName(basicType);
    }

    /** Full definition of the column of that type in {@link SampleKeyspaces#SCALARS} */
    ColumnSpec columnSpec() {
      return ColumnSpec.newBuilder()
          .setName(columnName())
          .setType(TypeSpec.newBuilder().setBasic(basicType).build())
          .build();
    }

    @Override
    public String toString() {
      return basicType + " " + graphqlLiteral;
    }
  }
}
