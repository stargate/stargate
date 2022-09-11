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
import java.sql.Date;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.util.TimeZone;

class TimestampCoercing extends TemporalCoercing<Instant> {

  static TimestampCoercing INSTANCE = new TimestampCoercing();

  /**
   * Patterns accepted by Apache Cassandra(R) 3.0 and higher when parsing CQL literals.
   *
   * <p>Note that Cassandra's TimestampSerializer declares many more patterns but some of them are
   * equivalent when parsing.
   */
  private static final String[] DATE_STRING_PATTERNS =
      new String[] {
        // 1) date-time patterns separated by 'T'
        // (declared first because none of the others are ISO compliant, but some of these are)
        // 1.a) without time zone
        "yyyy-MM-dd'T'HH:mm",
        "yyyy-MM-dd'T'HH:mm:ss",
        "yyyy-MM-dd'T'HH:mm:ss.SSS",
        // 1.b) with ISO-8601 time zone
        "yyyy-MM-dd'T'HH:mmX",
        "yyyy-MM-dd'T'HH:mmXX",
        "yyyy-MM-dd'T'HH:mmXXX",
        "yyyy-MM-dd'T'HH:mm:ssX",
        "yyyy-MM-dd'T'HH:mm:ssXX",
        "yyyy-MM-dd'T'HH:mm:ssXXX",
        "yyyy-MM-dd'T'HH:mm:ss.SSSX",
        "yyyy-MM-dd'T'HH:mm:ss.SSSXX",
        "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
        // 1.c) with generic time zone
        "yyyy-MM-dd'T'HH:mm z",
        "yyyy-MM-dd'T'HH:mm:ss z",
        "yyyy-MM-dd'T'HH:mm:ss.SSS z",
        // 2) date-time patterns separated by whitespace
        // 2.a) without time zone
        "yyyy-MM-dd HH:mm",
        "yyyy-MM-dd HH:mm:ss",
        "yyyy-MM-dd HH:mm:ss.SSS",
        // 2.b) with ISO-8601 time zone
        "yyyy-MM-dd HH:mmX",
        "yyyy-MM-dd HH:mmXX",
        "yyyy-MM-dd HH:mmXXX",
        "yyyy-MM-dd HH:mm:ssX",
        "yyyy-MM-dd HH:mm:ssXX",
        "yyyy-MM-dd HH:mm:ssXXX",
        "yyyy-MM-dd HH:mm:ss.SSSX",
        "yyyy-MM-dd HH:mm:ss.SSSXX",
        "yyyy-MM-dd HH:mm:ss.SSSXXX",
        // 2.c) with generic time zone
        "yyyy-MM-dd HH:mm z",
        "yyyy-MM-dd HH:mm:ss z",
        "yyyy-MM-dd HH:mm:ss.SSS z",
        // 3) date patterns without time
        // 3.a) without time zone
        "yyyy-MM-dd",
        // 3.b) with ISO-8601 time zone
        "yyyy-MM-ddX",
        "yyyy-MM-ddXX",
        "yyyy-MM-ddXXX",
        // 3.c) with generic time zone
        "yyyy-MM-dd z"
      };

  private final ThreadLocal<SimpleDateFormat> parser;

  private final ThreadLocal<SimpleDateFormat> formatter;

  private TimestampCoercing() {
    this(ZoneId.systemDefault());
  }

  private TimestampCoercing(ZoneId zoneId) {
    parser =
        ThreadLocal.withInitial(
            () -> {
              SimpleDateFormat parser = new SimpleDateFormat();
              parser.setLenient(false);
              parser.setTimeZone(TimeZone.getTimeZone(zoneId));
              return parser;
            });
    formatter =
        ThreadLocal.withInitial(
            () -> {
              SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
              parser.setTimeZone(TimeZone.getTimeZone(zoneId));
              return parser;
            });
  }

  @Override
  protected String format(Instant value) {
    return formatter.get().format(Date.from(value));
  }

  @Override
  protected Instant parse(String value) {
    SimpleDateFormat parser = this.parser.get();
    TimeZone timeZone = parser.getTimeZone();
    ParsePosition pos = new ParsePosition(0);
    for (String pattern : DATE_STRING_PATTERNS) {
      parser.applyPattern(pattern);
      pos.setIndex(0);
      try {
        java.util.Date date = parser.parse(value, pos);
        if (date != null && pos.getIndex() == value.length()) {
          return date.toInstant();
        }
      } finally {
        // restore the parser's default time zone, it might have been modified by the call to
        // parse()
        parser.setTimeZone(timeZone);
      }
    }
    throw new CoercingParseLiteralException("Cannot parse Timestamp value");
  }

  @Override
  protected Instant parse(long value) {
    return Instant.ofEpochMilli(value);
  }
}
