/*
 * Copyright The Stargate Authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package io.stargate.sgv2.docsapi.service.util;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Strings;
import io.stargate.sgv2.docsapi.api.exception.ErrorCode;
import io.stargate.sgv2.docsapi.api.exception.ErrorCodeRuntimeException;
import io.stargate.sgv2.docsapi.api.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.api.properties.document.DocumentTableProperties;
import io.stargate.sgv2.docsapi.config.constants.Constants;
import io.stargate.sgv2.docsapi.service.common.model.RowWrapper;
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

public final class DocsApiUtils {

  public static final Pattern PERIOD_PATTERN = Pattern.compile("(?<!\\\\)\\.");
  public static final Pattern COMMA_PATTERN = Pattern.compile("(?<!\\\\),");

  // Make these public if needed in the future
  private static final Pattern ARRAY_PATH_PATTERN = Pattern.compile("\\[.*\\]");
  private static final Pattern ARRAY_INDEX_PATTERN = Pattern.compile("\\[(\\d+)\\]");
  private static final Pattern ESCAPED_PATTERN = Pattern.compile("(\\\\,|\\\\\\.|\\\\\\*)");
  private static final Pattern ESCAPED_PATTERN_INTERNAL_CAPTURE =
      Pattern.compile("\\\\(\\.|\\*|,)");
  private static final Pattern BRACKET_SEPARATOR_PATTERN = Pattern.compile("\\]\\[");

  private DocsApiUtils() {
    // intentionally empty
  }

  /**
   * Given a path such as "a.b.c.[0]", converts into a JSON pointer for the JSON path "a.b.c.0"
   *
   * @param path
   * @return A JsonPointer that points to the given JSON path, or empty if path is null
   */
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
      if (!Objects.equals(innerPath, Constants.GLOB_VALUE)) {
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

  public static String leftPadTo6(String value) {
    return Strings.padStart(value, 6, '0');
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
   * Returns true if the String contains illegal characters. Used during writes to ensure that all
   * special characters are escaped.
   *
   * @param value A String
   * @return true if the String has an illegal character
   */
  public static boolean containsIllegalSequences(String value) {
    String replaced = ESCAPED_PATTERN.matcher(value).replaceAll("");
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

  public static String getStringFromRow(RowWrapper row, DocumentProperties properties) {
    String columnName = properties.tableProperties().stringValueColumnName();

    return row.isNull(columnName) ? null : row.getString(columnName);
  }

  public static Double getDoubleFromRow(RowWrapper row, DocumentProperties properties) {
    String columnName = properties.tableProperties().doubleValueColumnName();

    return row.isNull(columnName) ? null : row.getDouble(columnName);
  }

  public static Boolean getBooleanFromRow(
      RowWrapper row, DocumentProperties properties, boolean numericBooleans) {
    String columnName = properties.tableProperties().booleanValueColumnName();

    boolean nullValue = row.isNull(columnName);
    if (nullValue) {
      return null;
    } else {
      if (numericBooleans) {
        byte value = row.getByte(columnName);
        return value != 0;
      } else {
        return row.getBoolean(columnName);
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
   * @param properties {@link DocumentProperties}
   * @return True if row is matching on the given path
   */
  public static boolean isRowMatchingPath(
      RowWrapper row, List<String> path, DocumentProperties properties) {
    DocumentTableProperties tableProperties = properties.tableProperties();
    int targetPathSize = path.size();

    // short-circuit if the field is not matching
    // we expect leaf to be always fetched
    String field = path.get(targetPathSize - 1);
    String leaf = row.getString(tableProperties.leafColumnName());
    if (!Objects.equals(DocsApiUtils.convertEscapedCharacters(field), leaf)) {
      return false;
    }

    // short-circuit if p_n after path is not empty
    String targetP = tableProperties.pathColumnName(targetPathSize);
    boolean exists = row.columnExists(targetP);
    if (!exists || !Objects.equals(row.getString(targetP), "")) {
      return false;
    }

    // then as last resort confirm the path is matching
    return DocsApiUtils.isRowOnPath(row, path, properties);
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
  public static boolean isRowOnPath(
      RowWrapper row, Iterable<String> pathIterable, DocumentProperties properties) {
    DocumentTableProperties tableProperties = properties.tableProperties();

    int p = 0;
    for (String target : pathIterable) {
      int index = p++;
      // check that row has the request path depth
      String targetP = tableProperties.pathColumnName(index);
      boolean exists = row.columnExists(targetP);
      if (!exists) {
        return false;
      }

      // get the path
      String path = row.getString(targetP);

      // skip any target path that is a wildcard
      if (Objects.equals(target, Constants.GLOB_VALUE)) {
        continue;
      }

      // skip any target path that is an array wildcard
      if (Objects.equals(target, Constants.GLOB_ARRAY_VALUE)) {
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

  /**
   * Creates a new Map with the path values (p0...pN) already filled in. Since it is backed by a
   * LinkedHashMap, this can be used to preserve the data in the same order it was written, which
   * can be useful for binding values to queries.
   *
   * @param path The path segments for the shredded row
   * @param maxDepth
   * @param properties DocumentProperties configuration object
   * @return A Map (write order preserved upon iteration) of the bound data
   */
  public static Map<String, Object> newBindMap(
      List<String> path, int maxDepth, DocumentProperties properties) {
    Map<String, Object> bindMap = new LinkedHashMap<>(maxDepth + 5);

    bindMap.put(properties.tableProperties().keyColumnName(), null);

    for (int i = 0; i < maxDepth; i++) {
      String value = "";
      if (i < path.size()) {
        value = path.get(i);
      }
      bindMap.put(properties.tableProperties().pathColumnName(i), value);
    }

    bindMap.put(properties.tableProperties().leafColumnName(), null);
    bindMap.put(properties.tableProperties().stringValueColumnName(), null);
    bindMap.put(properties.tableProperties().doubleValueColumnName(), null);
    bindMap.put(properties.tableProperties().booleanValueColumnName(), null);

    return bindMap;
  }
}
