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

import graphql.language.IntValue;
import graphql.schema.Coercing;
import graphql.schema.CoercingParseLiteralException;
import graphql.schema.CoercingParseValueException;
import graphql.schema.CoercingSerializeException;
import java.math.BigInteger;

/**
 * Handles type conversions for custom scalars that map to "small" integer CQL types. We can map
 * these type to integer literals, both in GraphQL and JSON.
 *
 * <p>We follow the same rules as CQL: values that are out of range are rejected.
 *
 * @param <DriverTypeT> how the Java driver represents this type. This is what we'll deserialize to
 *     for incoming data, and will send back for results.
 * @see BigIntCoercing
 * @see VarintCoercing
 */
abstract class IntCoercing<DriverTypeT> implements Coercing<DriverTypeT, Long> {

  protected abstract DriverTypeT fromBigInteger(BigInteger i);

  protected abstract DriverTypeT fromNumber(Number n);

  @Override
  public Long serialize(Object dataFetcherResult) throws CoercingSerializeException {
    return ((Number) dataFetcherResult).longValue();
  }

  @Override
  public DriverTypeT parseValue(Object input) throws CoercingParseValueException {
    if (input instanceof BigInteger) {
      return fromBigIntegerSafe((BigInteger) input);
    } else if (input instanceof Integer || input instanceof Long) {
      return fromNumber((Number) input);
    } else {
      throw new CoercingParseLiteralException("Expected an integer literal");
    }
  }

  @Override
  public DriverTypeT parseLiteral(Object input) throws CoercingParseLiteralException {
    if (!(input instanceof IntValue)) {
      throw new CoercingParseLiteralException("Expected an integer literal");
    }
    IntValue value = (IntValue) input;
    return fromBigIntegerSafe(value.getValue());
  }

  private DriverTypeT fromBigIntegerSafe(BigInteger i) {
    try {
      return fromBigInteger(i);
    } catch (ArithmeticException e) {
      throw new CoercingParseLiteralException("Out of range");
    }
  }

  protected void checkRange(Number n, long min, long max) {
    long l = n.longValue();
    if (l < min || l > max) {
      throw new CoercingParseLiteralException("Out of range");
    }
  }

  static IntCoercing<Byte> TINYINT =
      new IntCoercing<Byte>() {
        @Override
        protected Byte fromBigInteger(BigInteger i) {
          return i.byteValueExact();
        }

        @Override
        protected Byte fromNumber(Number n) {
          checkRange(n, Byte.MIN_VALUE, Byte.MAX_VALUE);
          return n.byteValue();
        }
      };

  static IntCoercing<Short> SMALLINT =
      new IntCoercing<Short>() {
        @Override
        protected Short fromBigInteger(BigInteger i) {
          return i.shortValueExact();
        }

        @Override
        protected Short fromNumber(Number n) {
          checkRange(n, Short.MIN_VALUE, Short.MAX_VALUE);
          return n.shortValue();
        }
      };
}
