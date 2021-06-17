package io.stargate.graphql.schema.graphqlfirst.fetchers.deployed;

import graphql.schema.Coercing;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.graphql.schema.scalars.CqlScalar;
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
                return BIG_INT_COERCING.parseValue(environment.<String>getArgument(name));
              } else if (argument instanceof Integer) {
                return new Long(environment.<Integer>getArgument(name));
              } else {
                throw new IllegalStateException(
                    "The supported types for a timestamp value are String and Integer.");
              }
            });
  }
}
