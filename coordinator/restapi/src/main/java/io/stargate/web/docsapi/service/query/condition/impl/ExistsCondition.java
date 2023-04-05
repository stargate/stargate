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
import io.stargate.web.docsapi.service.query.filter.operation.FilterOperationCode;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * Exists condition is special type of the condition, as it does not provide {@link
 * io.stargate.db.query.builder.BuiltCondition}, but is considered as the persistence condition.
 * Also it has no filter operation and resolves each row test to true.
 */
@Value.Immutable
public abstract class ExistsCondition implements BaseCondition {

  /** @return Filter query value. */
  @Value.Parameter
  public abstract Boolean getQueryValue();

  /** {@inheritDoc} */
  @Override
  public boolean isPersistenceCondition() {
    return getQueryValue();
  }

  /** {@inheritDoc} */
  @Override
  public Optional<BuiltCondition> getBuiltCondition() {
    return Optional.empty();
  }

  /** {@inheritDoc} */
  @Override
  public FilterOperationCode getFilterOperationCode() {
    return FilterOperationCode.EXISTS;
  }

  /** {@inheritDoc} */
  @Override
  public boolean isEvaluateOnMissingFields() {
    return !getQueryValue();
  }

  /** {@inheritDoc} */
  @Override
  public boolean test(Row row) {
    // row must always be non-null here
    // if row exists then the test is true if query value is true
    return getQueryValue();
  }

  @Override
  public BaseCondition negate() {
    return ImmutableExistsCondition.of(!getQueryValue());
  }
}
