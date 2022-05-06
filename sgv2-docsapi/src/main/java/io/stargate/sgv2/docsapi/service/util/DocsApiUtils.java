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

import com.google.common.base.Strings;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentTableProperties;
import io.stargate.sgv2.docsapi.api.exception.ErrorCode;
import io.stargate.sgv2.docsapi.api.exception.ErrorCodeRuntimeException;
import io.stargate.sgv2.docsapi.config.constants.Constants;
import io.stargate.sgv2.docsapi.service.common.model.RowWrapper;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class DocsApiUtils {

  public static final Pattern PERIOD_PATTERN = Pattern.compile("(?<!\\\\)\\.");
  public static final Pattern COMMA_PATTERN = Pattern.compile("(?<!\\\\),");
  private static final Pattern ARRAY_PATH_PATTERN = Pattern.compile("\\[.*\\]");
  private static final Pattern ESCAPED_PATTERN_INTERNAL_CAPTURE =
      Pattern.compile("\\\\(\\.|\\*|,)");

  private DocsApiUtils() {
    // intentionally empty
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
}
