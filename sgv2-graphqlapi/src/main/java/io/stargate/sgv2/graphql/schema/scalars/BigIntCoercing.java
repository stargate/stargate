package io.stargate.sgv2.graphql.schema.scalars;

import graphql.language.IntValue;
import graphql.language.StringValue;
import graphql.schema.Coercing;
import graphql.schema.CoercingParseLiteralException;
import graphql.schema.CoercingParseValueException;
import graphql.schema.CoercingSerializeException;
import java.math.BigInteger;

/**
 * Handles type conversions for CQL {@code bigint}.
 *
 * <p>Our default mapping is String: client-side Javascript libraries cannot safely handle integers
 * outside of the range [-2^53, 2^53-1], so we can't return a Long.
 *
 * <p>For convenience, we also allow ints on the way in.
 */
class BigIntCoercing implements Coercing<Long, String> {

  static final BigIntCoercing INSTANCE = new BigIntCoercing();

  private BigIntCoercing() {}

  @Override
  public String serialize(Object dataFetcherResult) throws CoercingSerializeException {
    assert dataFetcherResult instanceof Long;
    return dataFetcherResult.toString();
  }

  @Override
  public Long parseValue(Object input) throws CoercingParseValueException {
    if (input instanceof BigInteger) {
      return fromBigInteger((BigInteger) input);
    } else if (input instanceof Integer || input instanceof Long) {
      return ((Number) input).longValue();
    } else if (input instanceof String) {
      return parse((String) input);
    } else {
      throw new CoercingParseLiteralException("Expected a string or integer literal");
    }
  }

  @Override
  public Long parseLiteral(Object input) throws CoercingParseLiteralException {
    if (input instanceof StringValue) {
      return parse(((StringValue) input).getValue());
    } else if (input instanceof IntValue) {
      return fromBigInteger(((IntValue) input).getValue());
    } else {
      throw new CoercingParseLiteralException("Expected a string or integer literal");
    }
  }

  private Long fromBigInteger(BigInteger input) {
    try {
      return input.longValueExact();
    } catch (ArithmeticException e) {
      throw new CoercingParseLiteralException("Out of bounds");
    }
  }

  private long parse(String input) {
    try {
      return Long.parseLong(input);
    } catch (NumberFormatException e) {
      throw new CoercingParseLiteralException("Invalid format");
    }
  }
}
