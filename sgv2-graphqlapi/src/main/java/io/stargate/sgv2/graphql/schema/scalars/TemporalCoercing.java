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
import graphql.language.StringValue;
import graphql.schema.Coercing;
import graphql.schema.CoercingParseLiteralException;
import graphql.schema.CoercingParseValueException;
import graphql.schema.CoercingSerializeException;
import java.math.BigInteger;

/**
 * Handle type conversions for temporal custom scalars. To reflect CQL's behavior, these types
 * accepts both a formatted string representation, as well as an long representation that represents
 * some kind of duration. On the way out, results are always formatted in the string representation.
 *
 * @param <DriverTypeT> how the Java driver represents this type. This is what we'll deserialize to
 *     for incoming data, and will send back for results.
 */
abstract class TemporalCoercing<DriverTypeT> implements Coercing<DriverTypeT, String> {

  protected abstract String format(DriverTypeT value);

  protected abstract DriverTypeT parse(String value);

  protected abstract DriverTypeT parse(long value);

  @Override
  public String serialize(Object dataFetcherResult) throws CoercingSerializeException {
    // The cast will always succeed since we've generated the GraphQL field type based on the CQL
    // column type
    @SuppressWarnings("unchecked")
    DriverTypeT value = (DriverTypeT) dataFetcherResult;
    return format(value);
  }

  @Override
  public DriverTypeT parseValue(Object input) throws CoercingParseValueException {
    if (input instanceof String) {
      String s = (String) input;
      return parseSafe(s);
    } else if (input instanceof BigInteger) {
      long longValue = fromBigIntegerSafe((BigInteger) input);
      return parseSafe(longValue);
    } else if (input instanceof Integer || input instanceof Long) {
      return parseSafe(((Number) input).longValue());
    } else {
      throw new CoercingParseLiteralException("Expected a string or integer literal");
    }
  }

  @Override
  public DriverTypeT parseLiteral(Object input) throws CoercingParseLiteralException {
    if (input instanceof StringValue) {
      return parseSafe(((StringValue) input).getValue());
    } else if (input instanceof IntValue) {
      BigInteger bi = ((IntValue) input).getValue();
      long longValue = fromBigIntegerSafe(bi);
      return parseSafe(longValue);
    } else {
      throw new CoercingParseLiteralException("Expected a string or integer literal");
    }
  }

  private DriverTypeT parseSafe(String s) {
    try {
      return parse(s);
    } catch (CoercingParseLiteralException e) {
      throw e;
    } catch (Exception e) {
      throw new CoercingParseLiteralException(e.getMessage(), e);
    }
  }

  private DriverTypeT parseSafe(long l) {
    try {
      return parse(l);
    } catch (CoercingParseLiteralException e) {
      throw e;
    } catch (Exception e) {
      throw new CoercingParseLiteralException(e.getMessage(), e);
    }
  }

  private long fromBigIntegerSafe(BigInteger i) {
    try {
      return i.longValueExact();
    } catch (ArithmeticException e) {
      throw new CoercingParseLiteralException("Out of bounds");
    }
  }
}
