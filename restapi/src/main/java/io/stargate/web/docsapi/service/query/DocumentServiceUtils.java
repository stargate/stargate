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

import io.stargate.web.docsapi.dao.DocumentDB;
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import java.util.Collection;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;

public final class DocumentServiceUtils {

  private static final Pattern ARRAY_PATH_PATTERN = Pattern.compile("\\[[0-9]+\\]");

  private DocumentServiceUtils() {}

  public static String convertArrayPath(String path) {
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
  public static long maxFieldDepth(String field) {
    return field.chars().filter(c -> c == '.').count() + 1;
  }

  /**
   * Calculates max depth for a collection of fields.
   *
   * @param fields fields
   * @return max depth or empty if no fields
   */
  public static OptionalLong maxFieldsDepth(Collection<String> fields) {
    return fields.stream().mapToLong(DocumentServiceUtils::maxFieldDepth).max();
  }
}
