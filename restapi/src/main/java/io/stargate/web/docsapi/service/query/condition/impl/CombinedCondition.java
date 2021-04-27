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

package io.stargate.web.docsapi.service.query.condition.impl;

import io.stargate.db.datastore.Row;
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.web.docsapi.service.query.condition.BaseCondition;
import io.stargate.web.docsapi.service.query.filter.operation.CombinedFilterOperation;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * Condition that works with the {@link CombinedFilterOperation} in order to match a single {@link
 * Row} against multiple database column values.
 *
 * @param <V>
 */
@Value.Immutable
public abstract class CombinedCondition<V> implements BaseCondition {

  /** @return Filter operation for the condition. */
  @Value.Parameter
  public abstract CombinedFilterOperation<V> getFilterOperation();

  /** @return Filter query value. */
  @Value.Parameter
  public abstract V getQueryValue();

  /** @return If booleans should be considered as numeric values. */
  @Value.Parameter
  public abstract boolean isNumericBooleans();

  /** Validates the value against the predicate. */
  @Value.Check
  protected void validate() {
    V queryValue = getQueryValue();
    getFilterOperation().validateBooleanFilterInput(queryValue);
    getFilterOperation().validateStringFilterInput(queryValue);
    getFilterOperation().validateDoubleFilterInput(queryValue);
  }

  /**
   * {@inheritDoc}
   *
   * <p>This implementation always returns empty. Sub-class to override.
   */
  @Override
  public Optional<BuiltCondition> getBuiltCondition() {
    return Optional.empty();
  }

  /** {@inheritDoc} */
  @Override
  public boolean test(Row row) {
    Boolean dbValueBoolean = getBooleanDatabaseValue(row, isNumericBooleans());
    Double dbValueDouble = getDoubleDatabaseValue(row);
    String dbValueString = getStringDatabaseValue(row);

    CombinedFilterOperation<V> filterOperation = getFilterOperation();
    V queryValue = getQueryValue();

    if (filterOperation.isMatchAll()) {
      return filterOperation.test(queryValue, dbValueBoolean)
          && filterOperation.test(queryValue, dbValueDouble)
          && filterOperation.test(queryValue, dbValueString);
    } else {
      return filterOperation.test(queryValue, dbValueBoolean)
          || filterOperation.test(queryValue, dbValueDouble)
          || filterOperation.test(queryValue, dbValueString);
    }
  }
}
