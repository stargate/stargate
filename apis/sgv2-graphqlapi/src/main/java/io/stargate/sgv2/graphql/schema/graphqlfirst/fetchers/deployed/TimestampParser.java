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
package io.stargate.sgv2.graphql.schema.graphqlfirst.fetchers.deployed;

import graphql.schema.DataFetchingEnvironment;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.Optional;

public class TimestampParser {

  public static Optional<Long> parse(
      Optional<String> cqlTimestampArgumentName, DataFetchingEnvironment environment) {
    return cqlTimestampArgumentName
        .filter(environment::containsArgument)
        .map(
            name -> {
              Object argument = environment.getArgument(name);
              if (argument instanceof Long) {
                return (Long) argument;
              } else if (argument instanceof String) {
                return parseString(((String) argument));
              } else {
                // Can't happen per the types allowed for the argument
                throw new AssertionError("Unexpected timestamp type");
              }
            });
  }

  private static long parseString(String spec) {
    try {
      ZonedDateTime dateTime = ZonedDateTime.parse(spec);
      return dateTime.toEpochSecond() * 1_000_000 + dateTime.getNano() / 1000;
    } catch (DateTimeParseException e2) {
      throw new IllegalArgumentException(
          String.format(
              "Can't parse Timeout '%s' (expected an ISO 8601 zoned date time string)", spec));
    }
  }
}
