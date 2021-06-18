package io.stargate.graphql.schema.graphqlfirst.fetchers.deployed;

import graphql.schema.Coercing;
import graphql.schema.CoercingParseLiteralException;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.graphql.schema.scalars.CqlScalar;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.Optional;

public class TimestampParser {
  @SuppressWarnings("unchecked")
  private static final Coercing<Long, String> BIG_INT_COERCING =
      CqlScalar.BIGINT.getGraphqlType().getCoercing();

  public static Optional<Long> parse(
      Optional<String> cqlTimestampArgumentName, DataFetchingEnvironment environment) {
    return cqlTimestampArgumentName
        .filter(environment::containsArgument)
        .map(
            name -> {
              Object argument = environment.getArgument(name);
              if (argument instanceof String) {
                return parseString(environment.getArgument(name));
              } else {
                throw new IllegalStateException(
                    "The supported type for a timestamp value is String.");
              }
            });
  }

  private static long parseString(String spec) {
    long microseconds;
    try {
      microseconds = BIG_INT_COERCING.parseValue(spec);
    } catch (CoercingParseLiteralException e) {
      ZonedDateTime dateTime = ZonedDateTime.parse(spec);
      microseconds = dateTime.toEpochSecond() * 1_000_000 + dateTime.getNano() / 1000;
    } catch (DateTimeParseException e2) {
      throw new IllegalArgumentException(
          String.format(
              "Can't parse Timeout '%s' (expected a BigInteger or ISO_ZONED_DATE_TIME string)",
              spec));
    }
    return microseconds;
  }
}
