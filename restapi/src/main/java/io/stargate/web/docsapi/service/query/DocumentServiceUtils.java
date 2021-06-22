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

package io.stargate.web.docsapi.service.query;

import io.stargate.db.datastore.Row;
import io.stargate.web.docsapi.dao.DocumentDB;
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

public final class DocumentServiceUtils {

  private static final Pattern ARRAY_PATH_PATTERN = Pattern.compile("\\[[0-9]+\\]");

  private DocumentServiceUtils() {}

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
          .map(DocumentServiceUtils::convertSingleArrayPath)
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
   * Calculates max depth for a field.
   *
   * @param field field
   * @return max depth
   */
  public static int maxFieldDepth(String field) {
    return field.chars().filter(c -> c == '.').map(e -> 1).sum() + 1;
  }

  /**
   * Calculates max depth for a collection of fields.
   *
   * @param fields fields
   * @return max depth or empty if no fields
   */
  public static OptionalInt maxFieldsDepth(Collection<String> fields) {
    return fields.stream().mapToInt(DocumentServiceUtils::maxFieldDepth).max();
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
}
