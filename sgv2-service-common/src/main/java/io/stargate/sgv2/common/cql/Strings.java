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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class Strings {

  private static final Set<String> BUILT_IN_TYPES =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  "ascii",
                  "bigint",
                  "blob",
                  "boolean",
                  "counter",
                  "date",
                  "decimal",
                  "double",
                  "duration",
                  "float",
                  "inet",
                  "int",
                  "smallint",
                  "text",
                  "time",
                  "timestamp",
                  "timeuuid",
                  "tinyint",
                  "tuple",
                  "uuid",
                  "varchar",
                  "varint")));

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

  /**
   * Given the text description of a CQL type, quote any UDT names inside it. For example:
   *
   * <pre>{@code
   * map<text,Address> => map<text,"Address">
   * }</pre>
   *
   * The input string is assumed to be in "internal" form, ie exact case and unquoted. For
   * simplicity, UDTs will be systematically quoted, even when they are not case-sensitive.
   */
  public static String doubleQuoteUdts(String dataTypeName) {
    dataTypeName = dataTypeName.trim();
    if (dataTypeName.isEmpty()) {
      throw new IllegalArgumentException("Invalid empty type name");
    }

    int lastCharIdx = dataTypeName.length() - 1;
    if (dataTypeName.charAt(0) == '\'') {
      // The quote should be terminated, and we should have at least 1 character + the quotes,
      if (dataTypeName.charAt(lastCharIdx) != '\'' || dataTypeName.length() < 3) {
        throw new IllegalArgumentException(
            "Malformed type name (missing closing quote): " + dataTypeName);
      }
      return dataTypeName;
    }

    // Normally we shouldn't get double-quoted types in the input, but if we do handle them
    // correctly
    if (dataTypeName.charAt(0) == '"') {
      if (dataTypeName.charAt(lastCharIdx) != '"' || dataTypeName.length() < 3) {
        throw new IllegalArgumentException(
            "Malformed type name (missing closing quote): " + dataTypeName);
      }
      return dataTypeName;
    }

    int paramsIdx = dataTypeName.indexOf('<');
    if (paramsIdx < 0) {
      return BUILT_IN_TYPES.contains(dataTypeName) ? dataTypeName : quote(dataTypeName, '"');
    } else {
      String baseTypeName = dataTypeName.substring(0, paramsIdx).trim();
      if (dataTypeName.charAt(lastCharIdx) != '>') {
        throw new IllegalArgumentException(
            String.format(
                "Malformed type name: parameters for type %s are missing a closing '>'",
                baseTypeName));
      }
      String paramsString = dataTypeName.substring(paramsIdx + 1, lastCharIdx);
      List<String> parameters = splitParameters(paramsString, dataTypeName);
      // No need to quote baseTypeName. We know that anything that is parameterizable with `<>`
      // is case-insensitive.
      return baseTypeName
          + parameters.stream()
              .map(Strings::doubleQuoteUdts)
              .collect(Collectors.joining(",", "<", ">"));
    }
  }

  private static List<String> splitParameters(String parametersString, String fullTypeName) {
    int openParam = 0;
    int currentStart = 0;
    int idx = currentStart;
    List<String> parameters = new ArrayList<>();
    while (idx < parametersString.length()) {
      switch (parametersString.charAt(idx)) {
        case ',':
          // Ignore if we're within a sub-parameter.
          if (openParam == 0) {
            parameters.add(parametersString.substring(currentStart, idx));
            currentStart = idx + 1;
          }
          break;
        case '<':
          ++openParam;
          break;
        case '>':
          if (--openParam < 0) {
            throw new IllegalArgumentException(
                "Malformed type name: " + fullTypeName + " (unmatched closing '>')");
          }
          break;
        case '"':
          idx = findClosingDoubleQuote(fullTypeName, parametersString, idx + 1) - 1;
      }
      ++idx;
    }
    parameters.add(parametersString.substring(currentStart, idx));
    return parameters;
  }

  // Returns the index "just after the double quote", so possibly str.length.
  private static int findClosingDoubleQuote(String fullTypeName, String str, int startIdx) {
    int idx = startIdx;
    while (idx < str.length()) {
      if (str.charAt(idx) == '"') {
        // Note: 2 double-quote is a way to escape the double-quote, so move to next first and
        // only exit if it's not a double-quote. Otherwise, continue.
        ++idx;
        if (idx >= str.length() || str.charAt(idx) != '"') {
          return idx;
        }
      }
      ++idx;
    }
    throw new IllegalArgumentException("Malformed type name: " + fullTypeName);
  }
}
