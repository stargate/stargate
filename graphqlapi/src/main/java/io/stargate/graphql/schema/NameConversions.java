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
package io.stargate.graphql.schema;

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
    if (type == IdentifierType.TABLE) {
      for (Map.Entry<String, String> entry : RESERVED_GRAPHQL_NAMES.entrySet()) {
        if (cqlName.equals(entry.getKey())) {
          return entry.getValue();
        }
      }
    }
    String result = hexEscape(cqlName);
    if (type == IdentifierType.TABLE) {
      result = escapeReservedPrefix(result);
      result = escapeReservedSuffix(result);
    } else if (type == IdentifierType.UDT) {
      result = escapeReservedPrefix(result);
      // No need to escape suffixes because we append our own:
      result += "Udt";
    }
    return result;
  }

  public static String toCql(String graphqlName, IdentifierType type) {
    if (graphqlName == null || graphqlName.isEmpty()) {
      throw new IllegalArgumentException("GraphQL name must be non-null and not empty");
    }
    if (type == IdentifierType.UDT) {
      if (!graphqlName.endsWith("Udt")) {
        throw new IllegalArgumentException("GraphQL UDT name must end with 'Udt'");
      }
      graphqlName = graphqlName.substring(0, graphqlName.length() - 3);
    }
    return unHexEscape(graphqlName);
  }

  private static String hexEscape(String source) {
    StringBuilder result = null;
    int length = source.length();
    int i = 0;
    while (i < length) {
      char c = source.charAt(i);
      // Handle this case separately because it consumes two characters:
      if (i < length - 1 && Character.isSurrogatePair(c, source.charAt(i + 1))) {
        result = appendHex(result, source, i);
        i += 2;
      }
      // Other cases where we escape: leading double underscores, leading digit, character not
      // allowed in GraphQL, or 'x' followed by something that could be interpreted as a hex escape.
      else if ((i == 0 && length > 1 && c == '_' && source.charAt(1) == '_')
          || (i == 0 && isDigit(c))
          || !isValidGraphql(c)
          || (c == 'x' && lengthOfHexEscape(source, i) > 0)) {
        result = appendHex(result, source, i);
        i += 1;
      } else {
        result = appendSame(result, source, i);
        i += 1;
      }
    }
    return (result == null) ? source : result.toString();
  }

  private static String unHexEscape(String source) {
    StringBuilder result = null;
    int length = source.length();
    int i = 0;
    while (i < length) {
      char c = source.charAt(i);
      int l;
      if (c == 'x' && (l = lengthOfHexEscape(source, i)) > 0) {
        String hexString = source.substring(i + 1, i + l - 1);
        int codePoint = Integer.parseInt(hexString, 16);
        result = appendCodePoint(result, codePoint, source, i);
        i += l;
      } else {
        result = appendSame(result, source, i);
        i += 1;
      }
    }
    return (result == null) ? source : result.toString();
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

  // The goal is to avoid allocating the StringBuilder until we actually replace a character.
  private static StringBuilder appendSame(StringBuilder target, String source, int sourceIndex) {
    if (target == null) {
      return null;
    } else {
      return target.append(source.charAt(sourceIndex));
    }
  }

  private static StringBuilder appendCodePoint(
      StringBuilder target, int codePoint, String source, int sourceIndex) {
    if (target == null) {
      target = new StringBuilder(source.substring(0, sourceIndex));
    }
    return target.appendCodePoint(codePoint);
  }

  private static StringBuilder appendHex(StringBuilder target, String source, int sourceIndex) {
    if (target == null) {
      target = new StringBuilder(source.substring(0, sourceIndex));
    }
    String hex = Integer.toHexString(source.codePointAt(sourceIndex));
    return target.append('x').append(hex).append('_');
  }

  private static boolean isValidGraphql(char c) {
    return isDigit(c) || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_';
  }

  private static boolean isHex(char c) {
    return isDigit(c) || (c >= 'a' && c <= 'f');
  }

  private static boolean isDigit(char c) {
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
          .build();

  private static ImmutableMap<String, String> RESERVED_PREFIXES =
      ImmutableMap.<String, String>builder()
          .put("Entry", "Entrx79_")
          .put("Tuple", "Tuplx65_")
          .build();

  private static ImmutableMap<String, String> RESERVED_SUFFIXES =
      ImmutableMap.<String, String>builder()
          .put("Input", "Inpux74_")
          .put("Result", "Resulx74_")
          .put("Order", "Ordex72_")
          .put("Udt", "Udx74_")
          .build();
}
