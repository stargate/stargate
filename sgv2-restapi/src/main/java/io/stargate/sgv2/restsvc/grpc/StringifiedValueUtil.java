package io.stargate.sgv2.restsvc.grpc;

import io.stargate.bridge.proto.QueryOuterClass;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Helper class that deals with "Stringified" variants of structured values.
 *
 * <p>Note: most of the code copied from StargateV1 "Converters" ({@code
 * io.stargate.web.resources.Converters}), unchanged.
 */
public class StringifiedValueUtil {
  public static void decodeStringifiedCollection(
      String value,
      ToProtoValueCodec elementCodec,
      Collection<QueryOuterClass.Value> results,
      char openingBrace,
      char closingBrace) {
    // To be improved but let's start with:
    String typeDesc = (openingBrace == '[') ? "List" : "Set";

    int idx = skipSpaces(value, 0);
    if (idx >= value.length()) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid %s value '%s': at character %d expecting '%c' but got EOF",
              typeDesc, value, idx, openingBrace));
    }
    char c = value.charAt(idx++);
    if (c != openingBrace) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid %s value '%s': at character %d expecting '%s' but got '%c'",
              typeDesc, value, idx, openingBrace, c));
    }
    idx = skipSpaces(value, idx);
    if (idx >= value.length()) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid %s value '%s': at character %d expecting element or '%c' but got EOF",
              typeDesc, value, idx, closingBrace));
    }
    if (value.charAt(idx) == closingBrace) {
      return;
    }
    while (idx < value.length()) {
      int n = skipCqlValue(value, idx);
      if (n < 0) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid %s value '%s': invalid CQL value at character %d", typeDesc, value, idx));
      }

      results.add(
          elementCodec.protoValueFromStringified(handleSingleQuotes(value.substring(idx, n))));
      idx = n;

      idx = skipSpaces(value, idx);
      if (idx >= value.length()) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid %s value '%s': at character %d expecting ',' or '%c' but got EOF",
                typeDesc, value, idx, closingBrace));
      }
      if (value.charAt(idx) == closingBrace) {
        return;
      }
      if (value.charAt(idx++) != ',') {
        throw new IllegalArgumentException(
            String.format(
                "Invalid %s value '%s': at character %d expecting ',' but got '%c'",
                typeDesc, value, idx, value.charAt(idx)));
      }

      idx = skipSpaces(value, idx);
    }
    throw new IllegalArgumentException(
        String.format(
            "Invalid %s value '%s': missing closing '%s'", typeDesc, value, closingBrace));
  }

  public static void decodeStringifiedMap(
      String value,
      ToProtoValueCodec keyCodec,
      ToProtoValueCodec valueCodec,
      Collection<QueryOuterClass.Value> results) {
    int idx = skipSpaces(value, 0);
    if (idx >= value.length()) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid Map value '%s': at character %d expecting '{' but got EOF", value, idx));
    }
    if (value.charAt(idx++) != '{') {
      throw new IllegalArgumentException(
          String.format(
              "Invalid Map value '%s': at character %d expecting '{' but got '%c'",
              value, idx, value.charAt(idx)));
    }

    idx = skipSpaces(value, idx);

    if (idx >= value.length()) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid Map value '%s': at character %d expecting element or '}' but got EOF",
              value, idx));
    }
    if (value.charAt(idx) == '}') {
      return;
    }

    while (idx < value.length()) {
      int n = skipCqlValue(value, idx);
      if (n < 0) {
        throw new IllegalArgumentException(
            String.format("Invalid map value '%s': invalid CQL value at character %d", value, idx));
      }

      QueryOuterClass.Value k =
          keyCodec.protoValueFromStringified(handleSingleQuotes(value.substring(idx, n)));
      idx = n;

      idx = skipSpaces(value, idx);
      if (idx >= value.length()) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid map value '%s': at character %d expecting ':' but got EOF", value, idx));
      }
      if (value.charAt(idx) != ':') {
        throw new IllegalArgumentException(
            String.format(
                "Invalid map value '%s': at character %d expecting ':' but got '%c'",
                value, idx, value.charAt(idx)));
      }
      idx = skipSpaces(value, ++idx);

      n = skipCqlValue(value, idx);
      if (n < 0) {
        throw new IllegalArgumentException(
            String.format("Invalid map value '%s': invalid CQL value at character %d", value, idx));
      }

      QueryOuterClass.Value v =
          valueCodec.protoValueFromStringified(handleSingleQuotes(value.substring(idx, n)));
      idx = n;

      results.add(k);
      results.add(v);

      idx = skipSpaces(value, idx);
      if (idx >= value.length()) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid map value '%s': at character %d expecting ',' or '}' but got EOF",
                value, idx));
      }
      if (value.charAt(idx) == '}') {
        return;
      }
      if (value.charAt(idx++) != ',') {
        throw new IllegalArgumentException(
            String.format(
                "Invalid map value '%s': at character %d expecting ',' but got '%c'",
                value, idx, value.charAt(idx)));
      }

      idx = skipSpaces(value, idx);
    }
    throw new IllegalArgumentException(
        String.format("Invalid map value '%s': missing closing '}'", value));
  }

  // Quite a bit of similarities with Collection handler but not enough to
  // refactor
  public static void decodeStringifiedTuple(
      String value, List<ToProtoValueCodec> elementCodecs, List<QueryOuterClass.Value> results) {
    final int length = value.length();
    int idx = skipSpaces(value, 0);
    if (idx >= value.length()) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid Tuple value '%s': at character %d expecting '(' but got EOF", value, idx));
    }
    if (value.charAt(idx) != '(') {
      throw new IllegalArgumentException(
          String.format(
              "Invalid tuple value '%s': at character %d expecting '(' but got '%c'",
              value, idx, value.charAt(idx)));
    }

    idx++;
    idx = skipSpaces(value, idx);

    int fieldIndex = 0;
    while (idx < length) {
      if (value.charAt(idx) == ')') {
        idx = skipSpaces(value, idx + 1);
        if (idx == length) {
          return;
        }
        throw new IllegalArgumentException(
            String.format(
                "Invalid tuple value '%s': at character %d expecting EOF or blank, but got \"%s\"",
                value, idx, value.substring(idx)));
      }
      int n = skipCqlValue(value, idx);
      if (n < 0) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid tuple value '%s': invalid CQL value at field %d (character %d)",
                value, fieldIndex, idx));
      }

      String fieldValue = value.substring(idx, n);
      QueryOuterClass.Value v =
          elementCodecs.get(fieldIndex).protoValueFromStringified(handleSingleQuotes(fieldValue));
      results.add(v);
      idx = n;
      idx = skipSpaces(value, idx);
      if (idx == length) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid tuple value '%s': at field %d (character %d) expecting ',' or ')', but got EOF",
                value, fieldIndex, idx));
      }
      char c = value.charAt(idx);
      if (c == ')') {
        continue;
      }
      if (c != ',') {
        throw new IllegalArgumentException(
            String.format(
                "Invalid tuple value '%s': at field %d (character %d) expecting ',' but got '%c'",
                value, fieldIndex, idx, c));
      }
      ++idx; // skip ','

      idx = skipSpaces(value, idx);
      fieldIndex += 1;
    }
  }

  // Unfortunately lots of duplication with preceding Map handler; may want to
  // figure out ways to refactor
  public static void decodeStringifiedUDT(
      String value,
      Map<String, ToProtoValueCodec> fieldCodecs,
      String udtName,
      Map<String, QueryOuterClass.Value> results) {
    int idx = skipSpaces(value, 0);
    if (idx >= value.length()) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid UDT value '%s': at character %d expecting '{' but got EOF", value, idx));
    }
    if (value.charAt(idx++) != '{') {
      throw new IllegalArgumentException(
          String.format(
              "Invalid UDT value '%s': at character %d expecting '{' but got '%c'",
              value, idx, value.charAt(idx)));
    }

    idx = skipSpaces(value, idx);

    if (idx >= value.length()) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid UDT value '%s': at character %d expecting element or '}' but got EOF",
              value, idx));
    }
    if (value.charAt(idx) == '}') {
      return;
    }

    while (idx < value.length()) {
      int n = skipCqlValue(value, idx);
      if (n < 0) {
        throw new IllegalArgumentException(
            String.format("Invalid UDT value '%s': invalid CQL value at character %d", value, idx));
      }

      // Anything clever about UDT field names?
      final String fieldName = handleSingleQuotes(value.substring(idx, n));
      final ToProtoValueCodec valueCodec = fieldCodecs.get(fieldName);
      if (valueCodec == null) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid UDT '%s' value '%s': unrecognized field '%s'", udtName, value, fieldName));
      }

      idx = skipSpaces(value, n);
      if (idx >= value.length()) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid UDT value '%s': at character %d expecting ':' but got EOF", value, idx));
      }
      if (value.charAt(idx) != ':') {
        throw new IllegalArgumentException(
            String.format(
                "Invalid UDT value '%s': at character %d expecting ':' but got '%c'",
                value, idx, value.charAt(idx)));
      }
      idx = skipSpaces(value, ++idx);

      n = skipCqlValue(value, idx);
      if (n < 0) {
        throw new IllegalArgumentException(
            String.format("Invalid UDT value '%s': invalid CQL value at character %d", value, idx));
      }

      QueryOuterClass.Value v =
          valueCodec.protoValueFromStringified(handleSingleQuotes(value.substring(idx, n)));
      idx = n;

      results.put(fieldName, v);

      idx = skipSpaces(value, idx);
      if (idx >= value.length()) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid UDT value '%s': at character %d expecting ',' or '}' but got EOF",
                value, idx));
      }
      if (value.charAt(idx) == '}') {
        return;
      }
      if (value.charAt(idx++) != ',') {
        throw new IllegalArgumentException(
            String.format(
                "Invalid UDT value '%s': at character %d expecting ',' but got '%c'",
                value, idx, value.charAt(idx)));
      }

      idx = skipSpaces(value, idx);
    }
    throw new IllegalArgumentException(
        String.format("Invalid UDT value '%s': missing closing '}'", value));
  }

  // // // Public helper methods

  /**
   * Helper method that checks for optional apostrophe quoting for "stringified" (aka CQL) values;
   * and if any found, removes outermost ones and replaces possible inlined "double-apostrophes"
   * with single instances.
   *
   * @param value Value to check and process
   * @return Value after removing "extra" apostrophes
   */
  public static String handleSingleQuotes(String value) {
    final int len = value.length();
    if (len >= 2 && value.charAt(0) == '\'' && value.charAt(len - 1) == '\'') {
      // First; remove surrounding single quotes
      value = value.substring(1, len - 1);
      // Second, replace doubled-up single quotes
      if (value.indexOf('\'') >= 0) {
        value = value.replaceAll("''", "'");
      }
    }
    return value;
  }

  // // // Methods from "ParseUtils":

  private static int skipCqlValue(String toParse, int idx) {
    if (idx >= toParse.length()) {
      return -1;
    }

    if (isBlank(toParse.charAt(idx))) {
      return -1;
    }

    int cbrackets = 0;
    int sbrackets = 0;
    int parens = 0;
    boolean inString = false;

    do {
      char c = toParse.charAt(idx);
      if (inString) {
        if (c == '\'') {
          if (idx + 1 < toParse.length() && toParse.charAt(idx + 1) == '\'') {
            ++idx; // this is an escaped quote, skip it
          } else {
            inString = false;
            if (cbrackets == 0 && sbrackets == 0 && parens == 0) {
              return idx + 1;
            }
          }
        }
        // Skip any other character
      } else if (c == '\'') {
        inString = true;
      } else if (c == '{') {
        ++cbrackets;
      } else if (c == '[') {
        ++sbrackets;
      } else if (c == '(') {
        ++parens;
      } else if (c == '}') {
        if (cbrackets == 0) {
          return idx;
        }

        --cbrackets;
        if (cbrackets == 0 && sbrackets == 0 && parens == 0) {
          return idx + 1;
        }
      } else if (c == ']') {
        if (sbrackets == 0) {
          return idx;
        }

        --sbrackets;
        if (cbrackets == 0 && sbrackets == 0 && parens == 0) {
          return idx + 1;
        }
      } else if (c == ')') {
        if (parens == 0) {
          return idx;
        }

        --parens;
        if (cbrackets == 0 && sbrackets == 0 && parens == 0) {
          return idx + 1;
        }
      } else if (isBlank(c) || !isCqlIdentifierChar(c)) {
        if (cbrackets == 0 && sbrackets == 0 && parens == 0) {
          return idx;
        }
      }
    } while (++idx < toParse.length());

    if (inString || cbrackets != 0 || sbrackets != 0 || parens != 0) {
      return -1;
    }
    return idx;
  }

  private static int skipSpaces(String toParse, int idx) {
    while (idx < toParse.length() && isBlank(toParse.charAt(idx))) ++idx;
    return idx;
  }

  private static boolean isBlank(int c) {
    return c == ' ' || c == '\t' || c == '\n';
  }

  public static boolean isCqlIdentifierChar(int c) {
    return (c >= '0' && c <= '9')
        || (c >= 'a' && c <= 'z')
        || (c >= 'A' && c <= 'Z')
        || c == '-'
        || c == '+'
        || c == '.'
        || c == '_'
        || c == '&';
  }
}
