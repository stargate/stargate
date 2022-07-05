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
 * Handles type conversions for CQL {@code varint}.
 *
 * <p>Our default mapping is String: GraphQL int literals can in theory represent arbitrarily large
 * numbers, but both query variables and results use JSON, where large number can cause
 * interoperability issues (in particular, this happens with the playground web UI).
 *
 * <p>For convenience, we also allow ints on the way in: for inlined GraphQL literals this works for
 * any value, for JSON only for the ones that are representable as a JSON literal.
 */
class VarintCoercing implements Coercing<BigInteger, String> {

  static VarintCoercing INSTANCE = new VarintCoercing();

  private VarintCoercing() {}

  @Override
  public String serialize(Object dataFetcherResult) throws CoercingSerializeException {
    assert dataFetcherResult instanceof BigInteger;
    return dataFetcherResult.toString();
  }

  @Override
  public BigInteger parseValue(Object input) throws CoercingParseValueException {
    if (input instanceof BigInteger) {
      return (BigInteger) input;
    } else if (input instanceof Integer || input instanceof Long) {
      long longValue = ((Number) input).longValue();
      return BigInteger.valueOf(longValue);
    } else if (input instanceof String) {
      return parse((String) input);
    } else {
      throw new CoercingParseLiteralException("Expected a string or integer literal");
    }
  }

  @Override
  public BigInteger parseLiteral(Object input) throws CoercingParseLiteralException {
    if (input instanceof StringValue) {
      return parse(((StringValue) input).getValue());
    } else if (input instanceof IntValue) {
      return ((IntValue) input).getValue();
    } else {
      throw new CoercingParseLiteralException("Expected a string or integer literal");
    }
  }

  private BigInteger parse(String s) {
    try {
      return new BigInteger(s);
    } catch (NumberFormatException e) {
      throw new CoercingParseLiteralException("Invalid format");
    }
  }
}
