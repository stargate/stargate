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

package io.stargate.sgv2.docsapi.service.query.condition.impl;

import io.stargate.sgv2.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.docsapi.service.query.condition.BaseCondition;
import io.stargate.sgv2.docsapi.service.query.filter.operation.FilterOperationCode;
import io.stargate.sgv2.docsapi.service.query.filter.operation.GenericFilterOperation;
import io.stargate.sgv2.docsapi.service.util.ExtendedRow;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * Condition that works with the {@link GenericFilterOperation} in order to match a single {@link
 * ExtendedRow} against multiple database column values.
 *
 * @param <V>
 */
@Value.Immutable
public abstract class GenericCondition<V> implements BaseCondition {

  /** @return Filter operation for the condition. */
  @Value.Parameter
  public abstract GenericFilterOperation<V> getFilterOperation();

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
    getFilterOperation().validateFilterInput(queryValue);
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
  public FilterOperationCode getFilterOperationCode() {
    return getFilterOperation().getOpCode();
  }

  /** {@inheritDoc} */
  @Override
  public boolean isEvaluateOnMissingFields() {
    return getFilterOperation().isEvaluateOnMissingFields();
  }

  /** {@inheritDoc} */
  @Override
  public boolean test(ExtendedRow row) {
    Boolean dbValueBoolean = getBoolean(row, isNumericBooleans());
    Double dbValueDouble = getDouble(row);
    String dbValueString = getString(row);

    GenericFilterOperation<V> filterOperation = getFilterOperation();
    V queryValue = getQueryValue();

    // compare against the non-null values, fallback to text compare even if null
    if (null != dbValueBoolean) {
      return filterOperation.test(dbValueBoolean, queryValue);
    } else if (null != dbValueDouble) {
      return filterOperation.test(dbValueDouble, queryValue);
    } else {
      return filterOperation.test(dbValueString, queryValue);
    }
  }

  @Override
  public BaseCondition negate() {
    return ImmutableGenericCondition.of(
        getFilterOperation().negate(), getQueryValue(), isNumericBooleans());
  }
}
