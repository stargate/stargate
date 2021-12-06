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
package io.stargate.sgv2.common.cql;

public class Strings {
  /**
   * Quote the given string; single quotes are escaped. If the given string is null, this method
   * returns a quoted empty string ({@code ''}).
   *
   * @param value The value to quote.
   * @return The quoted string.
   */
  public static String quote(String value) {
    return quote(value, '\'');
  }

  /**
   * Quotes text and escapes any existing quotes in the text. {@code String.replace()} is a bit too
   * inefficient (see JAVA-67, JAVA-1262).
   *
   * @param text The text.
   * @param quoteChar The character to use as a quote.
   * @return The text with surrounded in quotes with all existing quotes escaped with (i.e. '
   *     becomes '')
   */
  private static String quote(String text, char quoteChar) {
    if (text == null || text.isEmpty()) return emptyQuoted(quoteChar);

    int nbMatch = 0;
    int start = -1;
    do {
      start = text.indexOf(quoteChar, start + 1);
      if (start != -1) ++nbMatch;
    } while (start != -1);

    // no quotes found that need to be escaped, simply surround in quotes and return.
    if (nbMatch == 0) return quoteChar + text + quoteChar;

    // 2 for beginning and end quotes.
    // length for original text
    // nbMatch for escape characters to add to quotes to be escaped.
    int newLength = 2 + text.length() + nbMatch;
    char[] result = new char[newLength];
    result[0] = quoteChar;
    result[newLength - 1] = quoteChar;
    int newIdx = 1;
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      if (c == quoteChar) {
        // escape quote with another occurrence.
        result[newIdx++] = c;
        result[newIdx++] = c;
      } else {
        result[newIdx++] = c;
      }
    }
    return new String(result);
  }

  /**
   * @param quoteChar " or '
   * @return A quoted empty string.
   */
  private static String emptyQuoted(char quoteChar) {
    // don't handle non quote characters, this is done so that these are interned and don't create
    // repeated empty quoted strings.
    assert quoteChar == '"' || quoteChar == '\'';
    if (quoteChar == '"') return "\"\"";
    else return "''";
  }
}
