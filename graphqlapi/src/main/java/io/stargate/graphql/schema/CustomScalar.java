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
package io.stargate.graphql.schema;

import com.google.common.base.Preconditions;
import graphql.language.ArrayValue;
import graphql.language.BooleanValue;
import graphql.language.EnumValue;
import graphql.language.FloatValue;
import graphql.language.IntValue;
import graphql.language.NullValue;
import graphql.language.ObjectValue;
import graphql.language.StringValue;
import graphql.language.Value;
import graphql.schema.Coercing;
import graphql.schema.CoercingParseLiteralException;
import graphql.schema.CoercingParseValueException;
import graphql.schema.CoercingSerializeException;
import graphql.schema.GraphQLScalarType;
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

/** Enum representation of all the custom scalars supported by Stargate. */
public enum CustomScalar {
  UUID(
      "Uuid",
      "The `Uuid` scalar type represents a CQL uuid as a string.",
      o -> java.util.UUID.fromString(String.valueOf(o)),
      Object::toString),
  TIMEUUID(
      "TimeUuid",
      "The `TimeUuid` scalar type represents a CQL timeuuid as a string.",
      o -> java.util.UUID.fromString(String.valueOf(o)),
      Object::toString),

  INET(
      "Inet",
      "The `Inet` scalar type represents a CQL inet as a string.",
      e -> {
        try {
          return InetAddress.getByName(String.valueOf(e));
        } catch (UnknownHostException e1) {
          throw new RuntimeException(e1);
        }
      },
      InetAddress::getHostAddress),
  DATE("Date", "", o -> java.sql.Date.valueOf(o.toString()), Object::toString),
  //    DURATION("Duration", com.datastax.driver.core.Duration.class, "Represents a duration. A
  // duration stores separately months, days, and seconds due to the fact that the number of days in
  // a month varies, and a day can have 23 or 25 hours if a daylight saving is involved.",
  //            o->com.datastax.driver.core.Duration.from(o.toString()),
  //            Object::toString),
  BIGINT(
      "BigInt",
      "The `BIGINT` scalar type represents a CQL bigint (64-bit signed integer) as a string.",
      e -> {
        if (e instanceof String) {
          return new Long((String) e);
        }
        if (e instanceof Long) {
          return e;
        }
        if (e instanceof Integer) {
          return ((Integer) e).longValue();
        }
        throw new NumberFormatException(
            String.format(
                "Expected string for bigint scalar, obtained %s", e.getClass().getName()));
      },
      Object::toString),
  COUNTER(
      "Counter",
      "The `COUNTER` scalar type represents a CQL counter (64-bit signed integer) as a string.",
      String::valueOf,
      Object::toString),
  ASCII(
      "Ascii",
      "The `Ascii` scalar type represents CQL ascii character values as a string.",
      String::valueOf,
      Object::toString),
  DECIMAL(
      "Decimal",
      "The `Decimal` scalar type represents a CQL decimal as a string.",
      o -> new BigDecimal(o.toString()),
      Object::toString),
  VARINT(
      "Varint",
      "The `Varint` scalar type represents a CQL varint as a string.",
      o -> new BigInteger(o.toString()),
      Object::toString),
  FLOAT(
      "Float32",
      "The `Float32` scalar type represents a CQL float (single-precision floating point values).",
      o -> Float.parseFloat(o.toString()),
      e -> e),
  BLOB(
      "Blob",
      "The `Blob` scalar type represents a CQL blob as a base64 encoded byte array.",
      o -> ByteBuffer.wrap(Base64.getDecoder().decode(o.toString())),
      o -> Base64.getEncoder().encode(o)),
  TIMESTAMP(
      "Timestamp",
      "The `Timestamp` scalar type represents a DateTime.",
      o -> Instant.parse((String) o),
      Object::toString),
  TIME(
      "Time",
      "The `Time` scalar type represents a local time.",
      o -> LocalTime.parse((String) o),
      Object::toString);

  private final GraphQLScalarType graphQLScalar;

  <T> CustomScalar(
      String graphQLName,
      String description,
      Function<Object, T> parser,
      Function<T, Object> serializer) {
    this(graphQLName, description, parser, serializer, o -> {});
  }

  <T> CustomScalar(
      String graphQLName,
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
