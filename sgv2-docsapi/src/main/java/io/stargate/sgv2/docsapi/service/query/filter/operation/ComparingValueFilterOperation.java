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

import java.util.Comparator;

/**
 * Helper interface that can be used by any standard predicate that depends on the value comparing.
 */
public interface ComparingValueFilterOperation extends ValueFilterOperation {

  // default comparators we are using, nulls last
  Comparator<String> STRING_COMPARATOR = Comparator.nullsLast(Comparator.naturalOrder());
  Comparator<Double> DOUBLE_COMPARATOR = Comparator.nullsLast(Comparator.naturalOrder());
  Comparator<Boolean> BOOLEAN_COMPARATOR = Comparator.nullsLast(Comparator.naturalOrder());

  /**
   * Method for the comparing predicates that resolves if predicate test is true or false. Note that
   * we always compare filter value against the DB value.
   *
   * @param compareValue comparison value
   * @return If this comparison value satisfies the predicate test
   */
  boolean isSatisfied(int compareValue);

  /**
   * Should null database values be compared. Defaults to <code>false</code>, sub-classes can
   * override.
   *
   * @return if <code>false</code> comparing with null db value will always test false, otherwise
   *     it's considering nulls as last in comparison
   */
  default boolean compareNulls() {
    return false;
  }

  /** {@inheritDoc} */
  @Override
  default boolean test(String dbValue, String filterValue) {
    if (null == dbValue && !compareNulls()) {
      return false;
    }

    int compare = STRING_COMPARATOR.compare(dbValue, filterValue);
    return isSatisfied(compare);
  }

  /** {@inheritDoc} */
  @Override
  default boolean test(Double dbValue, Number filterValue) {
    if (null == dbValue && !compareNulls()) {
      return false;
    }

    // TODO do we wanna have more sophisticated compare for the numbers
    int compare = DOUBLE_COMPARATOR.compare(dbValue, filterValue.doubleValue());
    return isSatisfied(compare);
  }

  /** {@inheritDoc} */
  @Override
  default boolean test(Boolean dbValue, Boolean filterValue) {
    if (null == dbValue && !compareNulls()) {
      return false;
    }

    int compare = BOOLEAN_COMPARATOR.compare(dbValue, filterValue);
    return isSatisfied(compare);
  }
}
