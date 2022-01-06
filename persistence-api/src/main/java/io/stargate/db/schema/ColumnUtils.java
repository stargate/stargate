/*
 * Copyright DataStax, Inc. and/or The Stargate Authors
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
package io.stargate.db.schema;

import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import java.util.EnumMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.immutables.value.Value;

public class ColumnUtils {
  private static final Pattern PATTERN_DOUBLE_QUOTE = Pattern.compile("\"", Pattern.LITERAL);
  private static final String ESCAPED_DOUBLE_QUOTE = Matcher.quoteReplacement("\"\"");
  private static final Pattern UNQUOTED_IDENTIFIER = Pattern.compile("[a-z][a-z0-9_]*");

  public static final Pattern WHITESPACE_PATTERN = Pattern.compile("(?U)\\s");

  @Value.Immutable(prehash = true)
  abstract static class Codecs {
    abstract TypeCodec codec();
  }

  public static final EnumMap<Column.Type, ColumnUtils.Codecs> CODECS =
      new EnumMap<>(Column.Type.class);

  static {
    CODECS.put(Column.Type.Ascii, ImmutableCodecs.builder().codec(TypeCodecs.ASCII).build());
    CODECS.put(Column.Type.Bigint, ImmutableCodecs.builder().codec(TypeCodecs.BIGINT).build());
    CODECS.put(Column.Type.Blob, ImmutableCodecs.builder().codec(TypeCodecs.BLOB).build());
    CODECS.put(Column.Type.Boolean, ImmutableCodecs.builder().codec(TypeCodecs.BOOLEAN).build());
    CODECS.put(Column.Type.Counter, ImmutableCodecs.builder().codec(TypeCodecs.COUNTER).build());
    CODECS.put(Column.Type.Date, ImmutableCodecs.builder().codec(TypeCodecs.DATE).build());
    CODECS.put(Column.Type.Decimal, ImmutableCodecs.builder().codec(TypeCodecs.DECIMAL).build());
    CODECS.put(Column.Type.Double, ImmutableCodecs.builder().codec(TypeCodecs.DOUBLE).build());
    CODECS.put(Column.Type.Duration, ImmutableCodecs.builder().codec(TypeCodecs.DURATION).build());
    CODECS.put(Column.Type.Float, ImmutableCodecs.builder().codec(TypeCodecs.FLOAT).build());
    CODECS.put(Column.Type.Inet, ImmutableCodecs.builder().codec(TypeCodecs.INET).build());
    CODECS.put(Column.Type.Int, ImmutableCodecs.builder().codec(TypeCodecs.INT).build());
    CODECS.put(Column.Type.Smallint, ImmutableCodecs.builder().codec(TypeCodecs.SMALLINT).build());
    CODECS.put(Column.Type.Text, ImmutableCodecs.builder().codec(TypeCodecs.TEXT).build());
    CODECS.put(Column.Type.Time, ImmutableCodecs.builder().codec(TypeCodecs.TIME).build());
    CODECS.put(
        Column.Type.Timestamp, ImmutableCodecs.builder().codec(TypeCodecs.TIMESTAMP).build());
    CODECS.put(Column.Type.Timeuuid, ImmutableCodecs.builder().codec(TypeCodecs.TIMEUUID).build());
    CODECS.put(Column.Type.Tinyint, ImmutableCodecs.builder().codec(TypeCodecs.TINYINT).build());
    CODECS.put(Column.Type.Uuid, ImmutableCodecs.builder().codec(TypeCodecs.UUID).build());
    CODECS.put(Column.Type.Varint, ImmutableCodecs.builder().codec(TypeCodecs.VARINT).build());
  }

  public static boolean containsWhitespace(String s) {
    return null != s && WHITESPACE_PATTERN.matcher(s).find();
  }

  public static String wrapIfContainsSpace(String s) {
    return ColumnUtils.containsWhitespace(s) ? "\"" + s + "\"" : s;
  }

  public static boolean isValidUnquotedIdentifier(String text) {
    return UNQUOTED_IDENTIFIER.matcher(text).matches();
  }

  /**
   * Given the raw (as stored internally) text of an identifier, return its CQL representation. That
   * is, unless the text is full lowercase and use only characters allowed in unquoted identifiers,
   * the result is double-quoted.
   */
  public static String maybeQuote(String text) {
    if (UNQUOTED_IDENTIFIER.matcher(text).matches() && !ReservedKeywords.isReserved(text)) {
      return text;
    }
    return '"' + PATTERN_DOUBLE_QUOTE.matcher(text).replaceAll(ESCAPED_DOUBLE_QUOTE) + '"';
  }
}
