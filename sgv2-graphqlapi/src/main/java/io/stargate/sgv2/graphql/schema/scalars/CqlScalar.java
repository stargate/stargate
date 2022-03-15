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
package io.stargate.sgv2.graphql.schema.scalars;

import graphql.schema.GraphQLScalarType;
import io.stargate.bridge.grpc.CqlDuration;
import io.stargate.bridge.proto.QueryOuterClass.TypeSpec;
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
      TypeSpec.Basic.UUID,
      java.util.UUID.class,
      GraphQLScalarType.newScalar()
          .name("Uuid")
          .coercing(StringCoercing.UUID)
          .description(
              "Represents a CQL `uuid` as a string.\n"
                  + "Example: \"fe86cad0-1965-11eb-84e6-ab660d2e8c9e\"")
          .build()),
  TIMEUUID(
      TypeSpec.Basic.TIMEUUID,
      java.util.UUID.class,
      GraphQLScalarType.newScalar()
          .name("TimeUuid")
          .coercing(StringCoercing.TIMEUUID)
          .description(
              "Represents a CQL `timeuuid` as a string.\n"
                  + "Example: \"fe86cad0-1965-11eb-84e6-ab660d2e8c9e\"")
          .build()),
  INET(
      TypeSpec.Basic.INET,
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
      TypeSpec.Basic.DATE,
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
      TypeSpec.Basic.DURATION,
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
      TypeSpec.Basic.BIGINT,
      Long.class,
      GraphQLScalarType.newScalar()
          .name("BigInt")
          .coercing(BigIntCoercing.INSTANCE)
          .description(
              "Represents a CQL `bigint` as an integer literal.\n"
                  + "This is a 64-bit signed integer.")
          .build()),
  COUNTER(
      TypeSpec.Basic.COUNTER,
      Long.class,
      GraphQLScalarType.newScalar()
          .name("Counter")
          .coercing(BigIntCoercing.INSTANCE)
          .description(
              "Represents a CQL `counter` as an integer literal.\n"
                  + "This is a 64-bit signed integer.")
          .build()),
  ASCII(
      TypeSpec.Basic.ASCII,
      String.class,
      GraphQLScalarType.newScalar()
          .name("Ascii")
          .coercing(StringCoercing.ASCII)
          .description(
              "Represents a CQL `ascii` as a string.\n"
                  + "An error will be thrown if the string contains non-ASCII characters.")
          .build()),
  DECIMAL(
      TypeSpec.Basic.DECIMAL,
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
      TypeSpec.Basic.VARINT,
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
      TypeSpec.Basic.FLOAT,
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
      TypeSpec.Basic.BLOB,
      ByteBuffer.class,
      GraphQLScalarType.newScalar()
          .name("Blob")
          .coercing(StringCoercing.BLOB)
          .description(
              "Represents a CQL `blob` as a base64 encoded string.\n"
                  + "Example: \"yv4=\" (for the hex value 0xCAFE)")
          .build()),
  SMALLINT(
      TypeSpec.Basic.SMALLINT,
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
      TypeSpec.Basic.TINYINT,
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
      TypeSpec.Basic.TIMESTAMP,
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
      TypeSpec.Basic.TIME,
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

  private static final Map<TypeSpec.Basic, CqlScalar> FROM_BASIC =
      Arrays.stream(values()).collect(Collectors.toMap(s -> s.basicCqlType, Function.identity()));
  private static final Map<String, CqlScalar> FROM_GRAPHQL_NAME =
      Arrays.stream(values())
          .collect(Collectors.toMap(s -> s.getGraphqlType().getName(), Function.identity()));

  private final TypeSpec.Basic basicCqlType;
  private final TypeSpec cqlType;
  private final Class<?> cqlValueClass;
  private final GraphQLScalarType graphqlType;

  CqlScalar(TypeSpec.Basic basicCqlType, Class<?> cqlValueClass, GraphQLScalarType graphqlType) {
    this.basicCqlType = basicCqlType;
    this.cqlType = TypeSpec.newBuilder().setBasic(basicCqlType).build();
    this.cqlValueClass = cqlValueClass;
    this.graphqlType = graphqlType;
  }

  public TypeSpec getCqlType() {
    return cqlType;
  }

  /** The Java type that the persistence API expects for this type. */
  public Class<?> getCqlValueClass() {
    return cqlValueClass;
  }

  public GraphQLScalarType getGraphqlType() {
    return graphqlType;
  }

  public static Optional<CqlScalar> fromBasicCqlType(TypeSpec.Basic cqlType) {
    return Optional.ofNullable(FROM_BASIC.get(cqlType));
  }

  public static Optional<CqlScalar> fromGraphqlName(String graphqlName) {
    return Optional.ofNullable(FROM_GRAPHQL_NAME.get(graphqlName));
  }
}
