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
package io.stargate.graphql.schema.scalars;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import graphql.schema.GraphQLScalarType;
import io.stargate.db.schema.Column;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A custom GraphQL scalar that mirror a CQL primitive type.
 *
 * <p>This is used in both the CQL-first and GraphQL-first APIs.
 */
public enum CqlScalar {
  UUID(
      Column.Type.Uuid,
      java.util.UUID.class,
      GraphQLScalarType.newScalar()
          .name("Uuid")
          .coercing(StringCoercing.UUID)
          .description(
              "Represents a CQL `uuid` as a string.\n"
                  + "Example: \"fe86cad0-1965-11eb-84e6-ab660d2e8c9e\"")
          .build()),
  TIMEUUID(
      Column.Type.Timeuuid,
      java.util.UUID.class,
      GraphQLScalarType.newScalar()
          .name("TimeUuid")
          .coercing(StringCoercing.TIMEUUID)
          .description(
              "Represents a CQL `timeuuid` as a string.\n"
                  + "Example: \"fe86cad0-1965-11eb-84e6-ab660d2e8c9e\"")
          .build()),
  INET(
      Column.Type.Inet,
      InetAddress.class,
      GraphQLScalarType.newScalar()
          .name("Inet")
          .coercing(StringCoercing.INET)
          .description(
              "Represents a CQL `inet` as a string.\n"
                  + "This is an IP address in IPv4 or IPv6 format.\n"
                  + "Examples: \"127.0.0.1\", \"::1\"\n"
                  + "If you don't provide a numerical address, it will be resolved on the server.")
          .build()),
  DATE(
      Column.Type.Date,
      LocalDate.class,
      GraphQLScalarType.newScalar()
          .name("Date")
          .coercing(DateCoercing.INSTANCE)
          .description(
              "Represents a CQL `date` as a string.\n"
                  + "This is a date without a time-zone in the ISO-8601 calendar system.\n"
                  + "Example: \"2020-10-21\".\n"
                  + "A date can also be input as a 32-bit, positive numeric literal, which will be "
                  + "interpreted as a number of days, where the epoch is in the middle of the range "
                  + "(1970-01-01 = 2147483648).")
          .build()),
  DURATION(
      Column.Type.Duration,
      CqlDuration.class,
      GraphQLScalarType.newScalar()
          .name("Duration")
          .coercing(StringCoercing.DURATION)
          .description(
              "Represents a CQL `duration` as a string.\n"
                  + "See https://docs.datastax.com/en/dse/6.8/cql/cql/cql_reference/upsertDates.html\n"
                  + "Example: \"1h4m48s20ms\"")
          .build()),
  BIGINT(
      Column.Type.Bigint,
      Long.class,
      GraphQLScalarType.newScalar()
          .name("BigInt")
          .coercing(BigIntCoercing.INSTANCE)
          .description(
              "Represents a CQL `bigint` as an integer literal.\n"
                  + "This is a 64-bit signed integer.")
          .build()),
  COUNTER(
      Column.Type.Counter,
      Long.class,
      GraphQLScalarType.newScalar()
          .name("Counter")
          .coercing(BigIntCoercing.INSTANCE)
          .description(
              "Represents a CQL `counter` as an integer literal.\n"
                  + "This is a 64-bit signed integer.")
          .build()),
  ASCII(
      Column.Type.Ascii,
      String.class,
      GraphQLScalarType.newScalar()
          .name("Ascii")
          .coercing(StringCoercing.ASCII)
          .description(
              "Represents a CQL `ascii` as a string.\n"
                  + "An error will be thrown if the string contains non-ASCII characters.")
          .build()),
  DECIMAL(
      Column.Type.Decimal,
      BigDecimal.class,
      GraphQLScalarType.newScalar()
          .name("Decimal")
          .coercing(StringCoercing.DECIMAL)
          .description(
              "Represents a CQL `decimal` as a string.\n"
                  + "This is a variable-precision decimal.\n"
                  + "Examples: \"1.5\", \"1e-3\"")
          .build()),
  VARINT(
      Column.Type.Varint,
      BigInteger.class,
      GraphQLScalarType.newScalar()
          .name("Varint")
          .coercing(VarintCoercing.INSTANCE)
          .description(
              "Represents a CQL `varint` as an integer.\n"
                  + "This is an arbitrary-precision integer.\n"
                  + "Examples: 1, 9223372036854775808")
          .build()),
  FLOAT(
      Column.Type.Float,
      Float.class,
      GraphQLScalarType.newScalar()
          .name("Float32")
          .coercing(FloatCoercing.INSTANCE)
          .description(
              "Represents a CQL `float` as a floating-point literal.\n"
                  + "This is a 32-bit IEEE-754 floating point.\n"
                  + "If the value cannot be represented as a float, it will be converted. "
                  + "This conversion can loose precision, or range (resulting in +/-Infinity).")
          .build()),
  BLOB(
      Column.Type.Blob,
      ByteBuffer.class,
      GraphQLScalarType.newScalar()
          .name("Blob")
          .coercing(StringCoercing.BLOB)
          .description(
              "Represents a CQL `blob` as a base64 encoded string.\n"
                  + "Example: \"yv4=\" (for the hex value 0xCAFE)")
          .build()),
  SMALLINT(
      Column.Type.Smallint,
      Short.class,
      GraphQLScalarType.newScalar()
          .name("SmallInt")
          .coercing(IntCoercing.SMALLINT)
          .description(
              "Represents a CQL `smallint` as an integer.\n"
                  + "This is a 16-bit signed int.\n"
                  + "An error will be thrown if the value is out of bounds.")
          .build()),
  TINYINT(
      Column.Type.Tinyint,
      Byte.class,
      GraphQLScalarType.newScalar()
          .name("TinyInt")
          .coercing(IntCoercing.TINYINT)
          .description(
              "Represents a CQL `tinyint` as an integer\n."
                  + "This is an 8-bit signed int.\n"
                  + "An error will be thrown if the value is out of bounds.")
          .build()),
  TIMESTAMP(
      Column.Type.Timestamp,
      Instant.class,
      GraphQLScalarType.newScalar()
          .name("Timestamp")
          .coercing(TimestampCoercing.INSTANCE)
          .description(
              "Represents a CQL `timestamp` as a string.\n"
                  + "This is an instantaneous point on the time-line.\n"
                  + "This type supports many different string representations "
                  + "(see https://docs.datastax.com/en/cql-oss/3.x/cql/cql_reference/timestamp_type_r.html). "
                  + "A timestamp can also be input as a numeric literal, which will be interpreted "
                  + "as a number of milliseconds since the epoch (1970-01-01 UTC at midnight).\n"
                  + "Examples: \"2011-02-03 04:05+0000\", 1296705900000\n"
                  + "String literals that do not include any time zone information will be "
                  + "interpreted using the server's default time zone.")
          .build()),
  TIME(
      Column.Type.Time,
      LocalTime.class,
      GraphQLScalarType.newScalar()
          .name("Time")
          .coercing(TimeCoercing.INSTANCE)
          .description(
              "Represents a CQL `time` as a string.\n"
                  + "This is a time without a time-zone in the ISO-8601 calendar system.\n"
                  + "Example: \"10:15:30.123456789\"\n"
                  + "A time can also be input as a numeric literal, which will be interpreted "
                  + "as a number of nanoseconds since midnight.")
          .build()),
  ;

  private static final Map<Column.Type, CqlScalar> FROM_CQL =
      Arrays.stream(values()).collect(Collectors.toMap(CqlScalar::getCqlType, Function.identity()));
  private static final Map<String, CqlScalar> FROM_GRAPHQL_NAME =
      Arrays.stream(values())
          .collect(Collectors.toMap(s -> s.getGraphqlType().getName(), Function.identity()));

  private final Column.Type cqlType;
  private final Class<?> cqlValueClass;
  private final GraphQLScalarType graphqlType;

  CqlScalar(Column.Type cqlType, Class<?> cqlValueClass, GraphQLScalarType graphqlType) {
    this.cqlType = cqlType;
    this.cqlValueClass = cqlValueClass;
    this.graphqlType = graphqlType;
  }

  public Column.Type getCqlType() {
    return cqlType;
  }

  /** The Java type that the persistence API expects for this type. */
  public Class<?> getCqlValueClass() {
    return cqlValueClass;
  }

  public GraphQLScalarType getGraphqlType() {
    return graphqlType;
  }

  public static Optional<CqlScalar> fromCqlType(Column.Type cqlType) {
    return Optional.ofNullable(FROM_CQL.get(cqlType));
  }

  public static Optional<CqlScalar> fromGraphqlName(String graphqlName) {
    return Optional.ofNullable(FROM_GRAPHQL_NAME.get(graphqlName));
  }
}
