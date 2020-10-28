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
package io.stargate.graphql.schema.types.scalars;

import graphql.language.FloatValue;
import graphql.language.IntValue;
import graphql.schema.Coercing;
import graphql.schema.CoercingParseLiteralException;
import graphql.schema.CoercingParseValueException;
import graphql.schema.CoercingSerializeException;
import java.math.BigDecimal;
import java.math.BigInteger;

class FloatCoercing implements Coercing<Float, Float> {

  static FloatCoercing INSTANCE = new FloatCoercing();

  private FloatCoercing() {}

  @Override
  public Float serialize(Object dataFetcherResult) throws CoercingSerializeException {
    return (Float) dataFetcherResult;
  }

  @Override
  public Float parseValue(Object input) throws CoercingParseValueException {
    if (input instanceof Double) {
      double d = (Double) input;
      float f = (float) d;
      if (f != d) {
        throw new CoercingParseLiteralException("Out of bounds");
      }
      return f;
    } else if (input instanceof BigInteger) {
      return checkBounds(((BigInteger) input).floatValue());
    } else if (input instanceof Long || input instanceof Integer) {
      long l = ((Number) input).longValue();
      // This may involve some rounding (see JLS 5.1.2)
      return (float) l;
    } else {
      throw new CoercingParseLiteralException("Expected an integer or float literal");
    }
  }

  @Override
  public Float parseLiteral(Object input) throws CoercingParseLiteralException {
    if (input instanceof IntValue) {
      BigInteger bi = ((IntValue) input).getValue();
      return checkBounds(bi.floatValue());
    } else if (input instanceof FloatValue) {
      BigDecimal bd = ((FloatValue) input).getValue();
      return checkBounds(bd.floatValue());
    } else {
      throw new CoercingParseLiteralException("Expected an integer or float literal");
    }
  }

  private float checkBounds(float f) {
    if (f == Float.NEGATIVE_INFINITY || f == Float.POSITIVE_INFINITY) {
      throw new CoercingParseLiteralException("Out of bounds");
    }
    return f;
  }
}
