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

package io.stargate.sgv2.docsapi.service.query.filter.operation;

import io.stargate.sgv2.api.common.cql.builder.Predicate;
import java.util.Optional;

/** Base predicate that can be used in the filter expression. */
public interface BaseFilterOperation {

  /**
   * @return Returns a {@link FilterOperationCode}.
   */
  FilterOperationCode getOpCode();

  /**
   * @return Mirrored persistence predicate, if one exists.
   */
  Optional<Predicate> getQueryPredicate();

  /**
   * Return <code>false</code> by default, sub-classes can override.
   *
   * @return if this filter operation should also evaluate on the missing field.
   */
  default boolean isEvaluateOnMissingFields() {
    return false;
  }

  /** Returns a filter operation that is the logical opposite of this operation. */
  BaseFilterOperation negate();
}
