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

package io.stargate.sgv2.docsapi.service.query.filter.operation.impl;

import io.stargate.sgv2.api.common.cql.builder.Predicate;
import io.stargate.sgv2.docsapi.service.query.filter.operation.FilterOperationCode;
import io.stargate.sgv2.docsapi.service.query.filter.operation.ValueFilterOperation;
import java.util.Optional;
import org.immutables.value.Value;

/** Greater than or equal filter operation. */
@Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
@Value.Immutable(singleton = true)
public abstract class GteFilterOperation extends NotNullValueFilterOperation {

  /**
   * @return Singleton instance
   */
  public static GteFilterOperation of() {
    return ImmutableGteFilterOperation.of();
  }

  /** {@inheritDoc} */
  @Override
  public FilterOperationCode getOpCode() {
    return FilterOperationCode.GTE;
  }

  /** {@inheritDoc} */
  @Override
  public Optional<Predicate> getQueryPredicate() {
    return Optional.of(Predicate.GTE);
  }

  /** {@inheritDoc} */
  @Override
  public boolean isSatisfied(int compareValue) {
    return compareValue >= 0;
  }

  @Override
  public ValueFilterOperation negate() {
    return LtFilterOperation.of();
  }
}
