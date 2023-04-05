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
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.web.docsapi.service.util;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.db.datastore.Row;
import io.stargate.db.query.TypedValue;
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import io.stargate.web.docsapi.service.DocsApiConfiguration;
import io.stargate.web.docsapi.service.query.DocsApiConstants;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

public final class DocsApiUtils {
  public static final Pattern PERIOD_PATTERN = Pattern.compile("(?<!\\\\)\\.");
  public static final Pattern COMMA_PATTERN = Pattern.compile("(?<!\\\\),");
  private static final Pattern ARRAY_PATH_PATTERN = Pattern.compile("\\[.*\\]");
  private static final Pattern ARRAY_INDEX_PATTERN = Pattern.compile("\\[(\\d+)\\]");
  public static final Pattern ESCAPED_PATTERN = Pattern.compile("(\\\\,|\\\\\\.|\\\\\\*)");
  public static final Pattern ESCAPED_PATTERN_INTERNAL_CAPTURE = Pattern.compile("\\\\(\\.|\\*|,)");
  public static final Pattern BRACKET_SEPARATOR_PATTERN = Pattern.compile("\\]\\[");

  private DocsApiUtils() {}

  public static Optional<JsonPointer> pathToJsonPointer(String path) {
    if (null == path) {
      return Optional.empty();
    }

    String separator = String.valueOf(JsonPointer.SEPARATOR);
    String adaptedPath =
        path.replaceAll(DocsApiUtils.PERIOD_PATTERN.pattern(), separator)
            .replaceAll(ARRAY_INDEX_PATTERN.pattern(), "$1");

    JsonPointer pointer = JsonPointer.compile(separator + adaptedPath);
    return Optional.of(pointer);
  }

  /**
   * Converts given path to an array one if needed:
   *
   * <ol>
   *   <li>[1] - converted to [000001]
   *   <li>[0],[1] - converted to [000000],[000001]
   *   <li>[*] - not converted
   *   <li>anyPath - not converted
   * </ol>
   *
   * @param path single filter or field path
   * @param maxArrayLength the maximum allowed array length
   * @return Converted to represent expected path in DB, keeping the segmentation.
   */
  public static String convertArrayPath(String path, int maxArrayLength) {
    if (path.contains(",")) {
      return Arrays.stream(COMMA_PATTERN.split(path))
          .map(pathVal -> DocsApiUtils.convertSingleArrayPath(pathVal, maxArrayLength))
          .collect(Collectors.joining(","));
    } else {
      return convertSingleArrayPath(path, maxArrayLength);
    }
  }

  /**
   * Alternative for DocsApiUtils#convertArrayPath(java.lang.String, int)
   *
   * @param path single filter or field path
   * @param config a {@link DocsApiConfiguration} that has a max array length defined
   * @return Converted to represent expected path in DB, keeping the segmentation.
   */
  public static String convertArrayPath(String path, DocsApiConfiguration config) {
    return convertArrayPath(path, config.getMaxArrayLength());
  }

  /**
   * Converts any of the valid escape sequences (for periods, commas, and asterisks) into their
   * actual character. E.g. if the input string is literally abc\.123, this function returns abc.123
   * This allows a user to use escape sequences in where filters and when writing documents when it
   * would otherwise be ambiguous to use the corresponding character.
   *
   * @param path single filter or field path
   * @return Converted to a string with no literal unicode code points.
   */
  public static String convertEscapedCharacters(String path) {
    return ESCAPED_PATTERN_INTERNAL_CAPTURE.matcher(path).replaceAll("$1");
  }

  public static List<String> convertEscapedCharacters(List<String> path) {
    return path.stream().map(DocsApiUtils::convertEscapedCharacters).collect(Collectors.toList());
  }

  private static String convertSingleArrayPath(String path, int maxArrayLength) {
    return extractArrayPathIndex(path, maxArrayLength)
        .map(innerPath -> "[" + leftPadTo6(innerPath.toString()) + "]")
        .orElse(path);
  }

  /**
   * Optionally extracts the index of the array path. Returns empty if the path is not an array
   * path.
   *
   * @param path single filter or field path
   * @param maxArrayLength the maximum allowed array length
   * @return Array index or empty
   */
  public static Optional<Integer> extractArrayPathIndex(String path, int maxArrayLength) {
    // check if we have array path
    if (isArrayPath(path)) {
      String innerPath = path.substring(1, path.length() - 1);
      // if it's wildcard keep as it is
      if (!Objects.equals(innerPath, DocsApiConstants.GLOB_VALUE)) {
        // otherwise try to parse int
        try {
          // this can fail, thus wrap in the try
          int idx = Integer.parseInt(innerPath);
          if (idx > maxArrayLength - 1) {
            String msg = String.format("Max array length of %s exceeded.", maxArrayLength);
            throw new ErrorCodeRuntimeException(
                ErrorCode.DOCS_API_GENERAL_ARRAY_LENGTH_EXCEEDED, msg);
          }
          return Optional.of(idx);
        } catch (NumberFormatException e) {
          String msg = String.format("Array path %s is not valid.", path);
          throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_SEARCH_ARRAY_PATH_INVALID, msg);
        }
      }
    }
    return Optional.empty();
  }

  /**
   * Returns true if this path is denoting an array element index.
   *
   * @param path single filter or field path
   * @return if it's array path
   */
  public static boolean isArrayPath(String path) {
    return ARRAY_PATH_PATTERN.matcher(path).matches();
  }

  /**
   * Alternative for DocsApiUtils#extractArrayPathIndex(java.lang.String, int)
   *
   * @param path single filter or field path
   * @param config a {@link DocsApiConfiguration} containing the maximum array length
   * @return Array index or empty
   */
  public static Optional<Integer> extractArrayPathIndex(String path, DocsApiConfiguration config) {
    return extractArrayPathIndex(path, config.getMaxArrayLength());
  }

  public static String leftPadTo6(String value) {
    return StringUtils.leftPad(value, 6, '0');
  }

  /**
   * Returns the collection of paths that are represented by each JsonNode in the #fieldsJson.
   *
   * <p>For each field a list of strings representing the path is returned. Note that for each path
   * member, {@link #convertArrayPath(String, int)} will be called.
   *
   * @param fieldsJson array json node
   * @param maxArrayLength the maximum allowed array length, from configuration
   * @return collection of paths representing all fields
   */
  public static Collection<List<String>> convertFieldsToPaths(
      JsonNode fieldsJson, int maxArrayLength) {
    if (!fieldsJson.isArray()) {
      String msg = String.format("`fields` must be a JSON array, found %s", fieldsJson);
      throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_GENERAL_FIELDS_INVALID, msg);
    }

    Collection<List<String>> results = new ArrayList<>();
    for (JsonNode value : fieldsJson) {
      if (!value.isTextual()) {
        String msg = String.format("Each field must be a string, found %s", value);
        throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_GENERAL_FIELDS_INVALID, msg);
      }
      String fieldValue = value.asText();
      List<String> fieldPath =
          Arrays.stream(PERIOD_PATTERN.split(fieldValue))
              .map(p -> DocsApiUtils.convertArrayPath(p, maxArrayLength))
              .map(DocsApiUtils::convertEscapedCharacters)
              .collect(Collectors.toList());

      results.add(fieldPath);
    }

    return results;
  }

  /**
   * Alternative for DocsApiUtils#convertFieldsToPaths(JsonNode, int)
   *
   * @param fieldsJson array json node
   * @param config a {@link DocsApiConfiguration} containing the max array length
   * @return collection of paths representing all fields
   */
  public static Collection<List<String>> convertFieldsToPaths(
      JsonNode fieldsJson, DocsApiConfiguration config) {
    return convertFieldsToPaths(fieldsJson, config.getMaxArrayLength());
  }

  public static boolean containsIllegalSequences(String x) {
    String replaced = ESCAPED_PATTERN.matcher(x).replaceAll("");
    return replaced.contains("[")
        || replaced.contains(".")
        || replaced.contains("'")
        || replaced.contains("\\");
  }

  /**
   * Converts a JSON path string (e.g. "$.a.b.c[0]") into a JSON path string that only uses square
   * brackets to denote pathing (e.g. "$['a']['b']['c'][0]". This is to allow escaping of certain
   * characters, such as space, $, and @.
   */
  public static String convertJsonToBracketedPath(String path) {
    String[] parts = PERIOD_PATTERN.split(path);
    StringBuilder newPath = new StringBuilder();
    for (int i = 0; i < parts.length; i++) {
      String part = parts[i];
      if (part.startsWith("$") && i == 0) {
        newPath.append(part);
      } else {
        int indexOfBrace = part.indexOf('[');
        if (indexOfBrace < 0) {
          newPath.append("['").append(part).append("']");
        } else {
          String keyPart = part.substring(0, indexOfBrace);
          String arrayPart = part.substring(indexOfBrace);
          newPath.append("['").append(keyPart).append("']").append(arrayPart);
        }
      }
    }
    return newPath.toString();
  }

  /**
   * Takes a bracketedPath as generated by DocsApiUtils#convertJsonToBracketedPath(java.lang.String)
   * and converts it to an array where each element is a piece of the path.
   *
   * <p>E.g. if bracketedPath is $['a'][0]['b'], then the array generated is ['a', 0, 'b']
   *
   * @param bracketedPath A bracket-separated path generated from
   *     DocsApiUtils#convertJsonToBracketedPath
   * @return an array of Strings where each element is a piece of the path
   */
  public static String[] bracketedPathAsArray(String bracketedPath) {
    // All bracketed paths start with $[ and end with ], so they must have at least length 3
    if (bracketedPath.length() < 3) {
      return new String[0];
    }

    return BRACKET_SEPARATOR_PATTERN.split(bracketedPath.substring(2, bracketedPath.length() - 1));
  }

  /**
   * Simple utility to return a string value from doc api {@link Row}.
   *
   * @param row Row
   * @return String or null if a row column {@value DocsApiConstants#STRING_VALUE_COLUMN_NAME) is null.
   */
  public static String getStringFromRow(Row row) {
    return row.isNull(DocsApiConstants.STRING_VALUE_COLUMN_NAME)
        ? null
        : row.getString(DocsApiConstants.STRING_VALUE_COLUMN_NAME);
  }

  /**
   * Simple utility to return a double value from doc api {@link Row}.
   *
   * @param row Row
   * @return String or null if a row column {@value DocsApiConstants#DOUBLE_VALUE_COLUMN_NAME) is null.
   */
  public static Double getDoubleFromRow(Row row) {
    return row.isNull(DocsApiConstants.DOUBLE_VALUE_COLUMN_NAME)
        ? null
        : row.getDouble(DocsApiConstants.DOUBLE_VALUE_COLUMN_NAME);
  }

  /**
   * Simple utility to return boolean value from doc api {@link Row} based on number booleans value.
   *
   * @param row Row
   * @param numericBooleans Consider booleans as numeric
   * @return True, False or null if a row column {@value DocsApiConstants#BOOLEAN_VALUE_COLUMN_NAME) is null.
   */
  public static Boolean getBooleanFromRow(Row row, boolean numericBooleans) {
    boolean nullValue = row.isNull(DocsApiConstants.BOOLEAN_VALUE_COLUMN_NAME);
    if (nullValue) {
      return null;
    } else {
      if (numericBooleans) {
        byte value = row.getByte(DocsApiConstants.BOOLEAN_VALUE_COLUMN_NAME);
        return value != 0;
      } else {
        return row.getBoolean(DocsApiConstants.BOOLEAN_VALUE_COLUMN_NAME);
      }
    }
  }

  /**
   * Tests if the given row exactly matches the path, where path is defined by the list of strings.
   *
   * <p>This checks:
   *
   * <ol>
   *   <li>ignores globs in #pathIterable
   *   <li>handles correctly path segments in #pathIterable (f.e. ['first,second', 'value']) will
   *       match row on `first.value` or `second.value`
   *   <li>
   *   <li>proves that path is not sub-path of row's path
   * </ol>
   *
   * @param row Row
   * @param path path as iterable strings
   * @return True if row is matching on the given path
   */
  public static boolean isRowMatchingPath(Row row, List<String> path) {
    int targetPathSize = path.size();

    // short-circuit if the field is not matching
    // we expect leaf to be always fetched
    String field = path.get(targetPathSize - 1);
    String leaf = row.getString(DocsApiConstants.LEAF_COLUMN_NAME);
    if (!Objects.equals(DocsApiUtils.convertEscapedCharacters(field), leaf)) {
      return false;
    }

    // short-circuit if p_n after path is not empty
    String targetP = DocsApiConstants.P_COLUMN_NAME.apply(targetPathSize);
    boolean exists = row.columnExists(targetP);
    if (!exists || !Objects.equals(row.getString(targetP), "")) {
      return false;
    }

    // then as last resort confirm the path is matching
    return DocsApiUtils.isRowOnPath(row, path);
  }

  /**
   * Tests if the given row is on the path, where path is defined by the iterable of strings.
   *
   * <p>This checks:
   *
   * <ol>
   *   <li>ignores globs in #pathIterable
   *   <li>handles correctly path segments in #pathIterable (f.e. ['first,second', 'value']) will
   *       match row on `first.value` or `second.value`
   *   <li>
   * </ol>
   *
   * @param row Row
   * @param pathIterable path as iterable strings
   * @return True if row is fully on the given path
   */
  public static boolean isRowOnPath(Row row, Iterable<String> pathIterable) {
    int p = 0;
    for (String target : pathIterable) {
      int index = p++;
      // check that row has the request path depth
      String targetP = DocsApiConstants.P_COLUMN_NAME.apply(index);
      boolean exists = row.columnExists(targetP);
      if (!exists) {
        return false;
      }

      // get the path
      String path = row.getString(targetP);

      // skip any target path that is a wildcard
      if (Objects.equals(target, DocsApiConstants.GLOB_VALUE)) {
        continue;
      }

      // skip any target path that is an array wildcard
      if (Objects.equals(target, DocsApiConstants.GLOB_ARRAY_VALUE)) {
        // but make sure this is not an normal field
        if (!ARRAY_PATH_PATTERN.matcher(path).matches()) {
          return false;
        }
        continue;
      }

      boolean noneMatch =
          Arrays.stream(COMMA_PATTERN.split(target))
              .noneMatch(t -> Objects.equals(DocsApiUtils.convertEscapedCharacters(t), path));
      if (noneMatch) {
        return false;
      }
    }

    return true;
  }

  public static Map<String, Object> newBindMap(List<String> path, int maxDepth) {
    Map<String, Object> bindMap = new LinkedHashMap<>(maxDepth + 5);

    bindMap.put("key", TypedValue.UNSET);

    for (int i = 0; i < maxDepth; i++) {
      String value = "";
      if (i < path.size()) {
        value = path.get(i);
      }
      bindMap.put("p" + i, value);
    }

    bindMap.put(DocsApiConstants.LEAF_COLUMN_NAME, null);
    bindMap.put(DocsApiConstants.STRING_VALUE_COLUMN_NAME, null);
    bindMap.put(DocsApiConstants.DOUBLE_VALUE_COLUMN_NAME, null);
    bindMap.put(DocsApiConstants.BOOLEAN_VALUE_COLUMN_NAME, null);

    return bindMap;
  }
}
