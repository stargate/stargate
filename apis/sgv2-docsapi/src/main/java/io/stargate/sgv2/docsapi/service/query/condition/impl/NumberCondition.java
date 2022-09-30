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

import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.api.common.cql.builder.Term;
import io.stargate.sgv2.docsapi.api.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.service.common.model.RowWrapper;
import io.stargate.sgv2.docsapi.service.query.condition.BaseCondition;
import io.stargate.sgv2.docsapi.service.query.filter.operation.FilterOperationCode;
import io.stargate.sgv2.docsapi.service.query.filter.operation.ValueFilterOperation;
import java.util.Optional;
import org.apache.commons.lang3.tuple.Pair;
import org.immutables.value.Value;

/** Condition that accepts number filter values and compare against double database row value. */
@Value.Immutable
public abstract class NumberCondition implements BaseCondition {

  /** @return Filter operation for the condition. */
  @Value.Parameter
  public abstract ValueFilterOperation getFilterOperation();

  /** @return Filter query value. */
  @Value.Parameter
  public abstract Number getQueryValue();

  /** @return The reference to DocumentProperties */
  @Value.Parameter
  public abstract DocumentProperties documentProperties();

  @Override
  public Class<?> getQueryValueType() {
    return Number.class;
  }

  /** Validates the value against the predicate. */
  @Value.Check
  protected void validate() {
    getFilterOperation().validateNumberFilterInput(getQueryValue());
  }

  /** {@inheritDoc} */
  @Override
  public Optional<Pair<BuiltCondition, QueryOuterClass.Value>> getBuiltCondition() {
    String column = documentProperties().tableProperties().doubleValueColumnName();
    return getFilterOperation()
        .getQueryPredicate()
        .map(
            predicate -> {
              QueryOuterClass.Value value = Values.of(getQueryValue().doubleValue());
              BuiltCondition condition = BuiltCondition.of(column, predicate, Term.marker());
              return Pair.of(condition, value);
            });
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
  public boolean test(RowWrapper row) {
    Double dbValue = getDouble(row);
    return getFilterOperation().test(dbValue, getQueryValue());
  }

  @Override
  public BaseCondition negate() {
    return ImmutableNumberCondition.of(
        getFilterOperation().negate(), getQueryValue(), documentProperties());
  }
}
