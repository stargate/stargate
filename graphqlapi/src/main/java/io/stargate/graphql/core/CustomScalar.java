package io.stargate.graphql.core;
/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

import com.google.common.base.Preconditions;
import graphql.language.*;
import graphql.schema.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalTime;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/** * Enum representation of all the custom scalars supported by AppStax */
public enum CustomScalar {
  UUID(
      "Uuid",
      java.util.UUID.class,
      "The `Uuid` scalar type represents a CQL uuid as a string.",
      o -> java.util.UUID.fromString(String.valueOf(o)),
      Object::toString),
  TIMEUUID(
      "TimeUuid",
      java.util.UUID.class,
      "The `TimeUuid` scalar type represents a CQL timeuuid as a string.",
      o -> java.util.UUID.fromString(String.valueOf(o)),
      Object::toString),

  INET(
      "Inet",
      InetAddress.class,
      "The `Inet` scalar type represents a CQL inet as a string.",
      e -> {
        try {
          return InetAddress.getByName(String.valueOf(e));
        } catch (UnknownHostException e1) {
          throw new RuntimeException(e1);
        }
      },
      InetAddress::getHostAddress),
  DATE("Date", java.sql.Date.class, "", o -> java.sql.Date.valueOf(o.toString()), Object::toString),
  //    DURATION("Duration", com.datastax.driver.core.Duration.class, "Represents a duration. A
  // duration stores separately months, days, and seconds due to the fact that the number of days in
  // a month varies, and a day can have 23 or 25 hours if a daylight saving is involved.",
  //            o->com.datastax.driver.core.Duration.from(o.toString()),
  //            Object::toString),
  BIGINT(
      "BigInt",
      Object.class,
      "The `BIGINT` scalar type represents a CQL bigint (64-bit signed integer) as a string.",
      e -> {
        if (e instanceof Long) {
          return BigInteger.valueOf((Long) e);
        } else if (e instanceof BigInteger) {
          return e;
        }
        return null;
      },
      e -> e.toString()),
  COUNTER(
      "Counter",
      String.class,
      "The `COUNTER` scalar type represents a CQL counter (64-bit signed integer) as a string.",
      String::valueOf,
      Object::toString),
  ASCII(
      "Ascii",
      String.class,
      "The `Ascii` scalar type represents CQL ascii character values as a string.",
      String::valueOf,
      Object::toString),
  DECIMAL(
      "Decimal",
      BigDecimal.class,
      "The `Decimal` scalar type represents a CQL decimal as a string.",
      o -> new BigDecimal(o.toString()),
      Object::toString),
  VARINT(
      "Varint",
      BigInteger.class,
      "The `Varint` scalar type represents a CQL varint as a string.",
      o -> new BigInteger(o.toString()),
      Object::toString),
  FLOAT(
      "Float32",
      Float.class,
      "The `Float32` scalar type represents a CQL float (single-precision floating point values).",
      o -> Float.parseFloat(o.toString()),
      e -> e),
  BLOB(
      "Blob",
      ByteBuffer.class,
      "The `Blob` scalar type represents a CQL blob as a base64 encoded byte array.",
      o -> ByteBuffer.wrap(Base64.getDecoder().decode(o.toString())),
      o -> Base64.getEncoder().encode(o)),
  TIMESTAMP(
      "Timestamp",
      Instant.class,
      "The `Timestamp` scalar type represents a DateTime.",
      o -> Instant.parse((String) o),
      Object::toString),
  TIME(
      "Time",
      LocalTime.class,
      "The `Time` scalar type represents a local time.",
      o -> LocalTime.parse((String) o),
      Object::toString);

  private final GraphQLScalarType graphQLScalar;

  private <T> CustomScalar(
      String graphQLName,
      Class<T> javaType,
      String description,
      Function<Object, T> parser,
      Function<T, Object> serializer) {
    this(graphQLName, javaType, description, parser, serializer, o -> {});
  }

  private <T> CustomScalar(
      String graphQLName,
      Class<T> javaType,
      String description,
      Function<Object, T> parser,
      Function<T, Object> serializer,
      Consumer<T> validator) {
    this.graphQLScalar =
        buildGraphQLScalar(graphQLName, description, parser, serializer, validator);
  }

  public GraphQLScalarType getGraphQLScalar() {
    return graphQLScalar;
  }

  /**
   * Builds a custom GraphQL scalar for a given scalar. Most of the building is very straight
   * forward - the only complexity arises around building the coercing function that Java-GraphQL
   * expects for parsing and serializing the scalar values.
   *
   * @return {@link GraphQLScalarType} corresponding to provided scalar
   */
  private static GraphQLScalarType buildGraphQLScalar(
      String graphQLName,
      String description,
      final Function parser,
      final Function serializer,
      final Consumer validator) {
    GraphQLScalarType.Builder builder = GraphQLScalarType.newScalar();
    builder.name(graphQLName);
    builder.description(description);

    builder.coercing(
        new Coercing() {
          @Override
          public Object serialize(Object o) throws CoercingSerializeException {
            try {
              Preconditions.checkNotNull(o);
              if (o instanceof String) return o;
              return serializer.apply(o);
            } catch (RuntimeException ex) {
              throw new CoercingSerializeException(
                  "Cannot serialize value [" + ex.getMessage() + "]", ex);
            }
          }

          @Override
          public Object parseValue(Object o) throws CoercingParseValueException {
            try {
              Preconditions.checkNotNull(o);
              Object res = parser.apply(o);
              validator.accept(res);
              return res;
            } catch (RuntimeException ex) {
              throw new CoercingParseValueException(
                  "Cannot serialize value [" + ex.getMessage() + "]", ex);
            }
          }

          @Override
          public Object parseLiteral(Object value) throws CoercingParseLiteralException {
            if (value == null || !(value instanceof Value))
              throw new CoercingParseLiteralException("Invalid value literal provided: " + value);
            try {
              Object o = literalValueToObject((Value) value);
              Object res = parser.apply(o);
              validator.accept(res);
              return res;
            } catch (RuntimeException ex) {
              throw new CoercingParseLiteralException(
                  "Cannot serialize value [" + ex.getMessage() + "]", ex);
            }
          }
        });

    return builder.build();
  }

  public static Object literalValueToObject(Value value) {
    if (value instanceof StringValue) {
      return ((StringValue) value).getValue();
    }
    if (value instanceof IntValue) {
      return ((IntValue) value).getValue();
    }
    if (value instanceof FloatValue) {
      return ((FloatValue) value).getValue();
    }
    if (value instanceof BooleanValue) {
      return ((BooleanValue) value).isValue();
    }
    if (value instanceof EnumValue) {
      return ((EnumValue) value).getName();
    }
    if (value instanceof NullValue) {
      return null;
    }
    if (value instanceof ArrayValue) {
      return ((ArrayValue) value)
          .getValues().stream()
              .map(CustomScalar::literalValueToObject)
              .collect(Collectors.toList());
    }
    if (value instanceof ObjectValue) {
      Map<String, Object> map = new LinkedHashMap<>();
      ((ObjectValue) value)
          .getObjectFields()
          .forEach(field -> map.put(field.getName(), literalValueToObject(field.getValue())));
      return map;
    }

    throw new IllegalArgumentException("Unknown scalar AST type: " + value.getClass().getName());
  }
}
