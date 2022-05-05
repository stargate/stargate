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

import io.stargate.sgv2.docsapi.service.query.DocsApiConstants;

public final class DocsApiUtils {

  private DocsApiUtils() {
    // intentionally empty
  }

  public static String getStringFromRow(ExtendedRow row) {
    return row.isNull(DocsApiConstants.STRING_VALUE_COLUMN_NAME)
        ? null
        : row.getString(DocsApiConstants.STRING_VALUE_COLUMN_NAME);
  }

  public static Double getDoubleFromRow(ExtendedRow row) {
    return row.isNull(DocsApiConstants.DOUBLE_VALUE_COLUMN_NAME)
        ? null
        : row.getDouble(DocsApiConstants.DOUBLE_VALUE_COLUMN_NAME);
  }

  public static Boolean getBooleanFromRow(ExtendedRow row, boolean numericBooleans) {
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
}
