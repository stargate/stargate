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

package io.stargate.web.docsapi.service.query.filter.operation.impl;

import io.stargate.db.query.Predicate;
import io.stargate.web.docsapi.service.query.filter.operation.FilterOperationCode;
import io.stargate.web.docsapi.service.query.filter.operation.GenericFilterOperation;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * Not in list filter operation. Note that this extends {@link InFilterOperation} and negates the
 * test results.
 */
@Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
@Value.Immutable(singleton = true)
public abstract class NotInFilterOperation extends InFilterOperation {

  /** @return Singleton instance */
  public static NotInFilterOperation of() {
    return ImmutableNotInFilterOperation.of();
  }

  /** {@inheritDoc} */
  @Override
  public FilterOperationCode getOpCode() {
    return FilterOperationCode.NIN;
  }

  /** {@inheritDoc} */
  @Override
  public Optional<Predicate> getQueryPredicate() {
    return Optional.empty();
  }

  /** {@inheritDoc} */
  @Override
  public boolean isEvaluateOnMissingFields() {
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public boolean test(String dbValue, List<?> filterValue) {
    return !super.test(dbValue, filterValue);
  }

  /** {@inheritDoc} */
  @Override
  public boolean test(Boolean dbValue, List<?> filterValue) {
    return !super.test(dbValue, filterValue);
  }

  /** {@inheritDoc} */
  @Override
  public boolean test(Double dbValue, List<?> filterValue) {
    return !super.test(dbValue, filterValue);
  }

  @Override
  public GenericFilterOperation<List<?>> negate() {
    return InFilterOperation.of();
  }
}
