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
package io.stargate.sgv2.api.common.cql;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ColumnUtils {

  private static final Pattern PATTERN_DOUBLE_QUOTE = Pattern.compile("\"", Pattern.LITERAL);
  private static final String ESCAPED_DOUBLE_QUOTE = Matcher.quoteReplacement("\"\"");
  private static final Pattern UNQUOTED_IDENTIFIER = Pattern.compile("[a-z][a-z0-9_]*");

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
