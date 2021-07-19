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

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.db.datastore.Row;
import io.stargate.web.docsapi.dao.DocumentDB;
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import io.stargate.web.docsapi.service.query.QueryConstants;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

public final class DocsApiUtils {

  private static final Pattern ARRAY_PATH_PATTERN = Pattern.compile("\\[.*\\]");

  private DocsApiUtils() {}

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
   * @return Converted to represent expected path in DB, keeping the segmentation.
   */
  public static String convertArrayPath(String path) {
    if (path.contains(",")) {
      return Arrays.stream(path.split(","))
          .map(DocsApiUtils::convertSingleArrayPath)
          .collect(Collectors.joining(","));
    } else {
      return convertSingleArrayPath(path);
    }
  }

  private static String convertSingleArrayPath(String path) {
    // check if we have array path
    if (ARRAY_PATH_PATTERN.matcher(path).matches()) {
      String innerPath = path.substring(1, path.length() - 1);
      // if it's wildcard keep as it is
      if (!Objects.equals(innerPath, DocumentDB.GLOB_VALUE)) {
        // otherwise try to parse int
        try {
          // this can fail, thus wrap in the try
          int idx = Integer.parseInt(innerPath);
          if (idx > DocumentDB.MAX_ARRAY_LENGTH - 1) {
            String msg =
                String.format("Max array length of %s exceeded.", DocumentDB.MAX_ARRAY_LENGTH);
            throw new ErrorCodeRuntimeException(
                ErrorCode.DOCS_API_GENERAL_ARRAY_LENGTH_EXCEEDED, msg);
          }
          return "[" + leftPadTo6(innerPath) + "]";
        } catch (NumberFormatException e) {
          String msg = String.format("Array path %s is not valid.", path);
          throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_SEARCH_ARRAY_PATH_INVALID, msg);
        }
      }
    }
    return path;
  }

  public static String leftPadTo6(String value) {
    return StringUtils.leftPad(value, 6, '0');
  }

  /**
   * Returns the collection of paths that are represented by each JsonNode in the #fieldsJson.
   *
   * <p>For each field a list of strings representing the path is returned. Note that for each path
   * member, {@link #convertArrayPath(String)} will be called.
   *
   * @param fieldsJson array json node
   * @return collection of paths representing all fields
   */
  public static Collection<List<String>> convertFieldsToPaths(JsonNode fieldsJson) {
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
          Arrays.stream(fieldValue.split("\\."))
              .map(DocsApiUtils::convertArrayPath)
              .collect(Collectors.toList());

      results.add(fieldPath);
    }

    return results;
  }

  /**
   * Simple utility to return a string value from doc api {@link Row}.
   *
   * @param row Row
   * @return String or null if a row column {@value QueryConstants#STRING_VALUE_COLUMN_NAME) is null.
   */
  public static String getStringFromRow(Row row) {
    return row.isNull(QueryConstants.STRING_VALUE_COLUMN_NAME)
        ? null
        : row.getString(QueryConstants.STRING_VALUE_COLUMN_NAME);
  }

  /**
   * Simple utility to return a double value from doc api {@link Row}.
   *
   * @param row Row
   * @return String or null if a row column {@value QueryConstants#DOUBLE_VALUE_COLUMN_NAME) is null.
   */
  public static Double getDoubleFromRow(Row row) {
    return row.isNull(QueryConstants.DOUBLE_VALUE_COLUMN_NAME)
        ? null
        : row.getDouble(QueryConstants.DOUBLE_VALUE_COLUMN_NAME);
  }

  /**
   * Simple utility to return boolean value from doc api {@link Row} based on number booleans value.
   *
   * @param row Row
   * @param numericBooleans Consider booleans as numeric
   * @return True, False or null if a row column {@value QueryConstants#BOOLEAN_VALUE_COLUMN_NAME) is null.
   */
  public static Boolean getBooleanFromRow(Row row, boolean numericBooleans) {
    boolean nullValue = row.isNull(QueryConstants.BOOLEAN_VALUE_COLUMN_NAME);
    if (nullValue) {
      return null;
    } else {
      if (numericBooleans) {
        byte value = row.getByte(QueryConstants.BOOLEAN_VALUE_COLUMN_NAME);
        return value != 0;
      } else {
        return row.getBoolean(QueryConstants.BOOLEAN_VALUE_COLUMN_NAME);
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
    String field = path.get(targetPathSize - 1);
    String leaf = row.getString(QueryConstants.LEAF_COLUMN_NAME);
    if (!Objects.equals(field, leaf)) {
      return false;
    }

    // short-circuit if p_n after path is not empty
    String afterPath = row.getString(QueryConstants.P_COLUMN_NAME.apply(targetPathSize));
    if (!Objects.equals(afterPath, "")) {
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
      String path = row.getString(QueryConstants.P_COLUMN_NAME.apply(index));
      if (null != path && path.length() == 0) {
        return false;
      }

      // skip any target path that is a wildcard
      if (Objects.equals(target, DocumentDB.GLOB_VALUE)) {
        continue;
      }

      // skip any target path that is an array wildcard
      if (Objects.equals(target, DocumentDB.GLOB_ARRAY_VALUE)) {
        // but make sure this is not an normal field
        if (!ARRAY_PATH_PATTERN.matcher(path).matches()) {
          return false;
        }
        continue;
      }

      boolean pathSegment = target.contains(",");
      // if we have the path segment, we need to check if any matches
      if (pathSegment) {
        boolean noneMatch =
            Arrays.stream(target.split(",")).noneMatch(t -> Objects.equals(t, path));
        if (noneMatch) {
          return false;
        }
      } else if (!Objects.equals(path, target)) {
        // if not equal, fail
        return false;
      }
    }

    return true;
  }
}
