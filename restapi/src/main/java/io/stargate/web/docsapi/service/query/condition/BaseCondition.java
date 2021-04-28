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

package io.stargate.web.docsapi.service.query.condition;

import io.stargate.db.datastore.Row;
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.web.docsapi.service.query.QueryConstants;

import java.util.Optional;
import java.util.function.Predicate;

/** Interface for the base filtering condition. */
public interface BaseCondition extends Predicate<Row> {

  /** @return If this condition can be executed on the persistence level. The default implementation resolves to true if the {@link #getBuiltCondition()} returns non-empty value.*/
  default boolean isPersistenceCondition() {
    return getBuiltCondition().isPresent();
  }

  /** @return Returns persistence built condition, if this condition supports database querying. */
  Optional<BuiltCondition> getBuiltCondition();

  /**
   * Resolves {@link String} value from the document {@link Row}.
   *
   * @param row Row
   * @return Returns resolved value or <code>null</code>
   */
  default String getString(Row row) {
    return row.isNull(QueryConstants.STRING_VALUE_COLUMN_NAME)
        ? null
        : row.getString(QueryConstants.STRING_VALUE_COLUMN_NAME);
  }

  /**
   * Resolves {@link Double} value from the document {@link Row}.
   *
   * @param row Row
   * @return Returns resolved value or <code>null</code>
   */
  default Double getDouble(Row row) {
    return row.isNull(QueryConstants.DOUBLE_VALUE_COLUMN_NAME)
        ? null
        : row.getDouble(QueryConstants.DOUBLE_VALUE_COLUMN_NAME);
  }

  /**
   * Resolves {@link Boolean} value from the document {@link Row}.
   *
   * @param row Row
   * @param numericBooleans If <code>true</code> assumes booleans are stored as numeric values.
   * @return Returns resolved value or <code>null</code>
   */
  default Boolean getBoolean(Row row, boolean numericBooleans) {
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
