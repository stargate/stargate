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
import io.stargate.web.docsapi.service.query.QueryConstants;
import io.stargate.web.docsapi.service.query.condition.BaseCondition;
import io.stargate.web.docsapi.service.query.filter.operation.StringValueFilterOperation;
import java.util.Optional;
import org.immutables.value.Value;

/** Condition that accepts string filter values and compare against string database row value. */
@Value.Immutable
public abstract class StringCondition implements BaseCondition {

  /** @return Predicate for the condition. */
  @Value.Parameter
  public abstract StringValueFilterOperation<String> getFilterOperation();

  /** @return Filter query value. */
  @Value.Parameter
  public abstract String getQueryValue();

  /** Validates the value against the predicate. */
  @Value.Check
  protected void validate() {
    getFilterOperation().validateStringFilterInput(getQueryValue());
  }

  /** {@inheritDoc} */
  @Override
  public Optional<BuiltCondition> getBuiltCondition() {
    return getFilterOperation()
        .getDatabasePredicate()
        .map(
            predicate ->
                BuiltCondition.of(
                    QueryConstants.STRING_VALUE_COLUMN_NAME, predicate, getQueryValue()));
  }

  /** {@inheritDoc} */
  @Override
  public boolean test(Row row) {
    String dbValue = getStringDatabaseValue(row);
    return getFilterOperation().test(getQueryValue(), dbValue);
  }
}
