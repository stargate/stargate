package io.stargate.sgv2.restsvc.grpc;

import io.stargate.proto.QueryOuterClass;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Helper class that deals with "Stringified" variants of structured values. */
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
    if (value.charAt(idx++) != openingBrace) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid %s value '%s': at character %d expecting '%s' but got '%c'",
              typeDesc, value, idx, openingBrace, value.charAt(idx)));
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

      results.add(elementCodec.protoValueFromStringified(value.substring(idx, n)));
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
      Collection<QueryOuterClass.Value> results)
  {
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

      QueryOuterClass.Value k = keyCodec.protoValueFromStringified(value.substring(idx, n));
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

      QueryOuterClass.Value v = valueCodec.protoValueFromStringified(value.substring(idx, n));
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
