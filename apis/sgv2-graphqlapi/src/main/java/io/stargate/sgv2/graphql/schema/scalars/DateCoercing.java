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

import graphql.schema.CoercingParseLiteralException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

class DateCoercing extends TemporalCoercing<LocalDate> {

  static DateCoercing INSTANCE = new DateCoercing();

  private static final LocalDate EPOCH = LocalDate.of(1970, 1, 1);

  private DateCoercing() {}

  @Override
  protected String format(LocalDate value) {
    return DateTimeFormatter.ISO_LOCAL_DATE.format(value);
  }

  @Override
  protected LocalDate parse(String value) {
    try {
      return LocalDate.parse(value, DateTimeFormatter.ISO_LOCAL_DATE);
    } catch (RuntimeException e) {
      throw new CoercingParseLiteralException(
          "Couldn't parse literal, expected ISO-8601 extended local date format (YYYY-MM-DD)");
    }
  }

  @Override
  protected LocalDate parse(long value) {
    int days = cqlDateToDaysSinceEpoch(value);
    return EPOCH.plusDays(days);
  }

  private static int cqlDateToDaysSinceEpoch(long raw) {
    if (raw < 0 || raw > MAX_CQL_LONG_VALUE)
      throw new CoercingParseLiteralException(
          String.format(
              "Numeric Date literals must be between 0 and %d (got %d)", MAX_CQL_LONG_VALUE, raw));
    return (int) (raw - EPOCH_AS_CQL_LONG);
  }

  private static final long MAX_CQL_LONG_VALUE = ((1L << 32) - 1);
  private static final long EPOCH_AS_CQL_LONG = (1L << 31);
}
