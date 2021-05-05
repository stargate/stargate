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
package io.stargate.graphql.schema.scalars;

import graphql.schema.CoercingParseLiteralException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

class TimeCoercing extends TemporalCoercing<LocalTime> {

  private static final DateTimeFormatter FORMATTER =
      DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSSSSS");

  static TimeCoercing INSTANCE = new TimeCoercing();

  private TimeCoercing() {}

  @Override
  protected String format(LocalTime value) {
    return FORMATTER.format(value);
  }

  @Override
  protected LocalTime parse(String value) {
    try {
      return LocalTime.parse(value);
    } catch (RuntimeException e) {
      throw new CoercingParseLiteralException(
          "Couldn't parse literal, expected ISO-8601 extended local time format (HH:MM:SS[s.sss])");
    }
  }

  @Override
  protected LocalTime parse(long value) {
    return LocalTime.ofNanoOfDay(value);
  }
}
