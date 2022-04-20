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
package io.stargate.sgv2.graphql.schema.cqlfirst.dml;

import com.google.common.collect.ImmutableMap;
import java.util.Map;

public class NameConversions {

  enum IdentifierType {
    TABLE,
    UDT,
    COLUMN,
  }

  private NameConversions() {}

  public static String toGraphql(String cqlName, IdentifierType type) {
    if (cqlName == null || cqlName.isEmpty()) {
      throw new IllegalArgumentException("CQL name must be non-null and not empty");
    }
    String graphqlName;
    if (type == IdentifierType.TABLE) {
      graphqlName = RESERVED_GRAPHQL_NAMES.get(cqlName);
      if (graphqlName != null) {
        return graphqlName;
      }
    }
    graphqlName = maybeHexEscape(cqlName);
    if (type == IdentifierType.TABLE) {
      graphqlName = escapeReservedPrefix(graphqlName);
      graphqlName = escapeReservedSuffix(graphqlName);
    } else if (type == IdentifierType.UDT) {
      graphqlName = escapeReservedPrefix(graphqlName);
      // No need to escape suffixes because we append our own:
      graphqlName += "Udt";
    }
    return graphqlName;
  }

  public static String toCql(String graphqlName, IdentifierType type) {
    if (graphqlName == null || graphqlName.isEmpty()) {
      throw new IllegalArgumentException("GraphQL name must be non-null and not empty");
    }
    if (type == IdentifierType.UDT && graphqlName.endsWith("Udt")) {
      graphqlName = graphqlName.substring(0, graphqlName.length() - 3);
    }
    return maybeHexUnescape(graphqlName);
  }

  private static String maybeHexEscape(String source) {
    // Note that we traverse the string twice in the worst case; but that's a fast linear iteration,
    // and not escaping should be the most common case anyway.
    return needsHexEscape(source) ? hexEscape(source) : source;
  }

  private static boolean needsHexEscape(String source) {
    assert source != null && !source.isEmpty();
    // GraphQL does not allow leading double underscores or digits
    if (source.startsWith("__") || isDigit(source.charAt(0))) {
      return true;
    }
    int i = 0, length = source.length();
    while (i < length) {
      int cp = source.codePointAt(i), charCount = Character.charCount(cp);
      if (charCount > 1 || !isValidGraphql(cp) || (cp == 'x' && lengthOfHexEscape(source, i) > 0)) {
        return true;
      }
      i += charCount;
    }
    return false;
  }

  private static String hexEscape(String source) {
    assert source != null && !source.isEmpty();
    StringBuilder result = new StringBuilder();
    int i = 0, length = source.length(), cp;
    if (source.startsWith("__")) {
      result.append("x5f__");
      i = 2;
    } else if (isDigit((cp = source.charAt(0)))) {
      appendHexEscape(result, cp);
      i = 1;
    }
    while (i < length) {
      cp = source.codePointAt(i);
      int charCount = Character.charCount(cp);
      if (charCount > 1 || !isValidGraphql(cp) || (cp == 'x' && lengthOfHexEscape(source, i) > 0)) {
        appendHexEscape(result, cp);
      } else {
        result.append((char) cp);
      }
      i += charCount;
    }
    return result.toString();
  }

  private static String maybeHexUnescape(String source) {
    return needsHexUnescape(source) ? hexUnescape(source) : source;
  }

  private static boolean needsHexUnescape(String source) {
    assert source != null && !source.isEmpty();
    int i = 0, length = source.length();
    while (i < length) {
      if (source.charAt(i) == 'x' && lengthOfHexEscape(source, i) > 0) {
        return true;
      }
      i += 1;
    }
    return false;
  }

  private static String hexUnescape(String source) {
    assert source != null && !source.isEmpty();
    StringBuilder result = new StringBuilder();
    int i = 0, length = source.length();
    while (i < length) {
      char c = source.charAt(i);
      int l;
      if (c == 'x' && (l = lengthOfHexEscape(source, i)) > 0) {
        String hexString = source.substring(i + 1, i + l - 1);
        result.appendCodePoint(Integer.parseInt(hexString, 16));
        i += l;
      } else {
        result.append(c);
        i += 1;
      }
    }
    return result.toString();
  }

  /**
   * @return the length of the hex escape starting at the current index (including the leading 'x'
   *     and trailing '_'), or -1 if there is none.
   */
  private static int lengthOfHexEscape(String name, int i) {
    assert name.charAt(i) == 'x';
    int j = i + 1;
    while (j < name.length()) {
      char c = name.charAt(j);
      if (c == '_') {
        // Must have at least one digit, 'x_' is not a valid hex escape.
        return (j == i + 1) ? -1 : j - i + 1;
      } else if (!isHex(c)) {
        return -1;
      }
      j += 1;
    }
    return -1;
  }

  private static void appendHexEscape(StringBuilder target, int codePoint) {
    target.append('x').append(Integer.toHexString(codePoint)).append('_');
  }

  private static boolean isValidGraphql(int c) {
    return isDigit(c) || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_';
  }

  private static boolean isHex(int c) {
    return isDigit(c) || (c >= 'a' && c <= 'f');
  }

  private static boolean isDigit(int c) {
    return c >= '0' && c <= '9';
  }

  private static String escapeReservedPrefix(String result) {
    for (Map.Entry<String, String> entry : RESERVED_PREFIXES.entrySet()) {
      String prefix = entry.getKey();
      String replacement = entry.getValue();
      if (result.startsWith(prefix)) {
        return replacement + result.substring(prefix.length());
      }
    }
    return result;
  }

  private static String escapeReservedSuffix(String result) {
    for (Map.Entry<String, String> entry : RESERVED_SUFFIXES.entrySet()) {
      String suffix = entry.getKey();
      String replacement = entry.getValue();
      if (result.endsWith(suffix)) {
        return result.substring(0, result.length() - suffix.length()) + replacement;
      }
    }
    return result;
  }

  private static final ImmutableMap<String, String> RESERVED_GRAPHQL_NAMES =
      ImmutableMap.<String, String>builder()
          // We use these names for our own types:
          .put("Mutation", "Mutatiox6e_")
          .put("MutationConsistency", "MutationConsistencx79_")
          .put("MutationOptions", "MutationOptionx73_")
          .put("Query", "Querx79_")
          .put("QueryConsistency", "QueryConsistencx79_")
          .put("QueryOptions", "QueryOptionx73_")
          .put("SerialConsistency", "SerialConsistencx79_")
          // Built-in scalars can clash with a table via "FilterInput" types
          .put("Boolean", "Booleax6e_")
          .put("Float", "Floax74_")
          .put("Int", "Inx74_")
          .put("String", "Strinx67_")
          // Custom scalars can clash with a table
          .put("Uuid", "Uuix64_")
          .put("TimeUuid", "TimeUuix64_")
          .put("Inet", "Inex74_")
          .put("Date", "Datx65_")
          .put("Duration", "Duratiox6e_")
          .put("BigInt", "BigInx74_")
          .put("Counter", "Countex72_")
          .put("Ascii", "Ascix69_")
          .put("Decimal", "Decimax6c_")
          .put("Varint", "Varinx74_")
          .put("Float32", "Float32")
          .put("Blob", "Blox62_")
          .put("SmallInt", "SmallInx74_")
          .put("TinyInt", "TinyInx74_")
          .put("Timestamp", "Timestamx70_")
          .put("Time", "Timx65_")
          .put("conversionWarnings", "conversionWarningx73_")
          .build();

  private static final ImmutableMap<String, String> RESERVED_PREFIXES =
      ImmutableMap.<String, String>builder()
          .put("Entry", "Entrx79_")
          .put("Tuple", "Tuplx65_")
          .build();

  private static final ImmutableMap<String, String> RESERVED_SUFFIXES =
      ImmutableMap.<String, String>builder()
          .put("Input", "Inpux74_")
          .put("Result", "Resulx74_")
          .put("Order", "Ordex72_")
          .put("Udt", "Udx74_")
          .build();
}
